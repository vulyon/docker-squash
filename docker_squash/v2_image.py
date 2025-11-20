import hashlib
import json
import os
import shutil
import tarfile
from collections import OrderedDict
from pathlib import Path
from typing import List, Tuple

from docker_squash.errors import SquashError
from docker_squash.image import Image


class V2Image(Image):
    FORMAT = "v2"

    def __init__(
        self, log, docker, image, from_layer, tmp_dir=None, tag=None, comment=""
    ):
        super().__init__(log, docker, image, from_layer, tmp_dir, tag, comment)

        # Check if image is a tar file path
        self.is_tar_input = self._is_tar_file(image)

        if self.is_tar_input:
            self.tar_path = image
            # For tar input, we don't need Docker client
            self.docker = None
            # Initialize directories early for tar processing
            self._initialize_directories()
            # Extract and process tar file
            self._extract_tar_image()
            self._detect_image_format()
            self._load_tar_metadata()
            self.size_before = self._dir_size(self.old_image_dir)

    def _is_tar_file(self, image_path):
        """Check if the provided image path is a tar file"""
        if not isinstance(image_path, str):
            return False

        # Check if it's a file path that exists and has tar extension or is a valid tar file
        if os.path.exists(image_path):
            if image_path.endswith((".tar", ".tar.gz", ".tgz")):
                return True
            # Try to open as tar file to verify
            try:
                with tarfile.open(image_path, "r"):
                    return True
            except (tarfile.TarError, OSError):
                return False
        return False

    def _extract_tar_image(self):
        """Extract tar image to temporary directory"""
        self.log.info(f"Extracting tar image from {self.tar_path}")

        if not os.path.exists(self.tar_path):
            raise SquashError(f"Tar file not found: {self.tar_path}")

        try:
            with tarfile.open(self.tar_path, "r") as tar:
                tar.extractall(self.old_image_dir)
        except Exception as e:
            raise SquashError(f"Failed to extract tar file: {e}")

        self.log.debug(f"Tar image extracted to {self.old_image_dir}")

    def _detect_image_format(self):
        """Detect if this is OCI format or Docker format"""
        index_file = os.path.join(self.old_image_dir, "index.json")
        manifest_file = os.path.join(self.old_image_dir, "manifest.json")

        if os.path.exists(index_file):
            self.log.info("Detected OCI format image")
            self.oci_format = True
        elif os.path.exists(manifest_file):
            self.log.info("Detected Docker format image")
            self.oci_format = False
        else:
            raise SquashError("Unable to detect image format - missing manifest files")

    def _load_tar_metadata(self):
        """Load image metadata from extracted tar"""
        if self.oci_format:
            self._load_oci_tar_metadata()
        else:
            self._load_docker_tar_metadata()

    def _load_oci_tar_metadata(self):
        """Load OCI format metadata from tar"""
        # Read index.json to get manifest reference
        index_file = os.path.join(self.old_image_dir, "index.json")
        with open(index_file, "r") as f:
            index_data = json.load(f, object_pairs_hook=OrderedDict)

        # Get the first manifest (assuming single image)
        if not index_data.get("manifests"):
            raise SquashError("No manifests found in index.json")

        manifest_desc = index_data["manifests"][0]
        manifest_digest = manifest_desc["digest"]

        # Read manifest from blobs
        manifest_path = os.path.join(
            self.old_image_dir, "blobs", "sha256", manifest_digest.split(":")[1]
        )
        if not os.path.exists(manifest_path):
            # Fallback to manifest.json if exists
            fallback_manifest = os.path.join(self.old_image_dir, "manifest.json")
            if os.path.exists(fallback_manifest):
                self.log.warning("Using fallback manifest.json for OCI image")
                self._load_docker_tar_metadata()
                return
            else:
                raise SquashError(f"Manifest blob not found: {manifest_path}")

        with open(manifest_path, "r") as f:
            manifest = json.load(f, object_pairs_hook=OrderedDict)

        # Handle nested index structure
        if manifest.get("mediaType") == "application/vnd.oci.image.index.v1+json":
            if not manifest.get("manifests"):
                raise SquashError("No manifests found in nested index")

            nested_manifest_desc = manifest["manifests"][0]
            nested_manifest_digest = nested_manifest_desc["digest"]
            nested_manifest_path = os.path.join(
                self.old_image_dir,
                "blobs",
                "sha256",
                nested_manifest_digest.split(":")[1],
            )

            if not os.path.exists(nested_manifest_path):
                raise SquashError(
                    f"Nested manifest blob not found: {nested_manifest_path}"
                )

            with open(nested_manifest_path, "r") as f:
                self.old_image_manifest = json.load(f, object_pairs_hook=OrderedDict)
        else:
            self.old_image_manifest = manifest

        # Read config blob
        if "config" not in self.old_image_manifest:
            raise SquashError("No config found in manifest - invalid OCI image")

        config_desc = self.old_image_manifest["config"]
        config_digest = config_desc["digest"]
        config_path = os.path.join(
            self.old_image_dir, "blobs", "sha256", config_digest.split(":")[1]
        )

        if not os.path.exists(config_path):
            raise SquashError(f"Config blob not found: {config_path}")

        with open(config_path, "r") as f:
            self.old_image_config = json.load(f, object_pairs_hook=OrderedDict)

        # Generate image ID from config hash
        self.old_image_id = f"sha256:{config_digest.split(':')[1]}"

    def _load_docker_tar_metadata(self):
        """Load Docker format metadata from tar"""
        manifest_file = os.path.join(self.old_image_dir, "manifest.json")
        with open(manifest_file, "r") as f:
            manifests = json.load(f, object_pairs_hook=OrderedDict)

        if not manifests:
            raise SquashError("Empty manifest.json")

        # Use the first manifest
        self.old_image_manifest = manifests[0]

        # Read config file
        config_path = os.path.join(
            self.old_image_dir, self.old_image_manifest["Config"]
        )
        with open(config_path, "r") as f:
            self.old_image_config = json.load(f, object_pairs_hook=OrderedDict)

        # Generate image ID from config hash
        config_content = json.dumps(
            self.old_image_config, sort_keys=True, separators=(",", ":")
        )
        self.old_image_id = (
            f"sha256:{hashlib.sha256(config_content.encode()).hexdigest()}"
        )

    def _before_squashing(self):
        if not self.is_tar_input:
            # For Docker daemon input, use parent's initialization
            super(V2Image, self)._before_squashing()

            # Read old image manifest file
            self.old_image_manifest = self._get_manifest()
            self.log.debug(
                f"Retrieved manifest {json.dumps(self.old_image_manifest, indent=4)}"
            )

            # Read old image config file
            self.old_image_config = self._read_json_file(
                os.path.join(self.old_image_dir, self.old_image_manifest["Config"])
            )
        else:
            # For tar input, metadata is already loaded in __init__
            # We need to set up the layer processing logic similar to base class
            self._setup_tar_layer_processing()

        # Read layer paths inside of the tar archive
        # We split it into layers that needs to be squashed
        # and layers that needs to be moved as-is
        self.layer_paths_to_squash: List[str] = []
        self.layer_paths_to_move: List[str] = []
        self.layer_paths_to_squash, self.layer_paths_to_move = self._read_layer_paths(
            self.old_image_config, self.old_image_manifest, self.layers_to_move
        )

        if self.layer_paths_to_move:
            self.squash_id = self.layer_paths_to_move[-1]

        self.log.debug(f"Layers paths to squash: {self.layer_paths_to_squash}")
        self.log.debug(f"Layers paths to move: {self.layer_paths_to_move}")

    def _setup_tar_layer_processing(self):
        """Setup layer processing for tar input similar to base class logic"""
        # Location of the tar archive with squashed layers
        self.squashed_tar = os.path.join(self.squashed_dir, "layer.tar")

        if self.tag:
            self.image_name, self.image_tag = self._parse_image_name(self.tag)

        # Build old_image_layers from config history for compatibility with base class logic
        self.old_image_layers = []
        self._build_layer_list_from_tar()

        self.log.info("Old image has %s layers", len(self.old_image_layers))
        self.log.debug("Old layers: %s", self.old_image_layers)

        # By default - squash all layers.
        if self.from_layer is None:
            self.from_layer = len(self.old_image_layers)

        try:
            number_of_layers = int(self.from_layer)
            self.log.debug(
                f"We detected number of layers ({number_of_layers}) as the argument to squash"
            )
        except ValueError:
            # For tar input, we need to adapt layer ID checking
            if self.from_layer in self.old_image_layers:
                number_of_layers = (
                    len(self.old_image_layers)
                    - self.old_image_layers.index(self.from_layer)
                    - 1
                )
            else:
                raise SquashError(
                    f"The {self.from_layer} layer could not be found in the image"
                )

        self._validate_number_of_layers(number_of_layers)

        marker = len(self.old_image_layers) - number_of_layers

        self.layers_to_squash = self.old_image_layers[marker:]
        self.layers_to_move = self.old_image_layers[:marker]

        self.log.info("Checking if squashing is necessary...")

        if len(self.layers_to_squash) < 1:
            raise SquashError(
                f"Invalid number of layers to squash: {len(self.layers_to_squash)}"
            )

        if len(self.layers_to_squash) == 1:
            from docker_squash.errors import SquashUnnecessaryError

            raise SquashUnnecessaryError(
                "Single layer marked to squash, no squashing is required"
            )

        self.log.info(f"Attempting to squash last {number_of_layers} layers...")
        self.log.debug(f"Layers to squash: {self.layers_to_squash}")
        self.log.debug(f"Layers to move: {self.layers_to_move}")

    def _build_layer_list_from_tar(self):
        """Build layer list from tar metadata similar to TarImage approach"""
        self.old_image_layers = []

        if self.oci_format:
            # Get actual layer digests from manifest (only non-empty layers)
            manifest_layers = []
            for layer_desc in self.old_image_manifest.get("layers", []):
                manifest_layers.append(layer_desc["digest"])

            # Build complete layer list from config.history (includes empty layers)
            manifest_layer_index = 0

            for i, history_entry in enumerate(self.old_image_config.get("history", [])):
                is_empty = history_entry.get("empty_layer", False)

                if is_empty:
                    # Empty layer - create a virtual layer ID
                    layer_id = f"<missing-{i}>"
                    self.old_image_layers.append(layer_id)
                else:
                    # Real layer - use digest from manifest
                    if manifest_layer_index < len(manifest_layers):
                        layer_id = manifest_layers[manifest_layer_index]
                        self.old_image_layers.append(layer_id)
                        manifest_layer_index += 1
                    else:
                        self.log.warning(f"Missing layer data for history entry {i}")
        else:
            # Docker format
            # Get actual layer paths from manifest (only non-empty layers)
            manifest_layers = self.old_image_manifest.get("Layers", [])
            manifest_layer_ids = []
            for layer_path in manifest_layers:
                # Extract layer ID from path (e.g., "abc123.../layer.tar" -> "abc123...")
                layer_id = layer_path.split("/")[0]
                manifest_layer_ids.append(f"sha256:{layer_id}")

            # Build complete layer list from config.history (includes empty layers)
            manifest_layer_index = 0

            for i, history_entry in enumerate(self.old_image_config.get("history", [])):
                is_empty = history_entry.get("empty_layer", False)

                if is_empty:
                    # Empty layer - create a virtual layer ID
                    layer_id = f"<missing-{i}>"
                    self.old_image_layers.append(layer_id)
                else:
                    # Real layer - use ID from manifest
                    if manifest_layer_index < len(manifest_layer_ids):
                        layer_id = manifest_layer_ids[manifest_layer_index]
                        self.old_image_layers.append(layer_id)
                        manifest_layer_index += 1
                    else:
                        self.log.warning(f"Missing layer data for history entry {i}")

    def _squash(self):
        if self.layer_paths_to_squash:
            # Prepare the directory
            os.makedirs(self.squashed_dir)

            # For tar input, filter out virtual layers before squashing
            if self.is_tar_input:
                real_layers_to_squash = [
                    layer_id
                    for layer_id in self.layer_paths_to_squash
                    if not layer_id.startswith("<missing-")
                ]
                real_layers_to_move = [
                    layer_id
                    for layer_id in self.layer_paths_to_move
                    if not layer_id.startswith("<missing-")
                ]

                if real_layers_to_squash:
                    self._squash_layers(real_layers_to_squash, real_layers_to_move)
                else:
                    self.log.info(
                        "No real layers to squash - all layers are empty/virtual"
                    )
            else:
                # Merge data layers
                self._squash_layers(
                    self.layer_paths_to_squash, self.layer_paths_to_move
                )

        self.diff_ids = self._generate_diff_ids()
        self.chain_ids = self._generate_chain_ids(self.diff_ids)

        metadata = self._generate_image_metadata()
        image_id = self._write_image_metadata(metadata)

        layer_path_id = None

        if self.layer_paths_to_squash:
            # Compute layer id to use to name the directory where
            # we store the layer data inside of the tar archive
            layer_path_id = self._generate_squashed_layer_path_id()

            if self.oci_format:
                old_layer_path = self.old_image_manifest["Config"]
            else:
                if self.layer_paths_to_squash[0]:
                    old_layer_path = self.layer_paths_to_squash[0]
                else:
                    old_layer_path = layer_path_id
                old_layer_path = os.path.join(old_layer_path, "json")

            metadata = self._generate_last_layer_metadata(layer_path_id, old_layer_path)
            self._write_squashed_layer_metadata(metadata)

            # Write version file to the squashed layer
            # Even Docker doesn't know why it's needed...
            self._write_version_file(self.squashed_dir)

            # Move the temporary squashed layer directory to the correct one
            shutil.move(
                self.squashed_dir, os.path.join(self.new_image_dir, layer_path_id)
            )

        manifest = self._generate_manifest_metadata(
            image_id,
            self.image_name,
            self.image_tag,
            self.old_image_manifest,
            self.layer_paths_to_move,
            layer_path_id,
        )

        self._write_manifest_metadata(manifest)

        repository_image_id = manifest[0]["Layers"][-1].split("/")[0]

        # Move all the layers that should be untouched
        self._move_layers(
            self.layer_paths_to_move, self.old_image_dir, self.new_image_dir
        )

        repositories_file = os.path.join(self.new_image_dir, "repositories")
        self._generate_repositories_json(
            repositories_file, repository_image_id, self.image_name, self.image_tag
        )

        return image_id

    def _write_image_metadata(self, metadata):
        # Create JSON from the metadata
        # Docker adds new line at the end
        json_metadata, image_id = self._dump_json(metadata, True)
        image_metadata_file = os.path.join(self.new_image_dir, "%s.json" % image_id)

        self._write_json_metadata(json_metadata, image_metadata_file)

        return image_id

    def _write_squashed_layer_metadata(self, metadata):
        layer_metadata_file = os.path.join(self.squashed_dir, "json")
        json_metadata = self._dump_json(metadata)[0]

        self._write_json_metadata(json_metadata, layer_metadata_file)

    def _write_manifest_metadata(self, manifest):
        manifest_file = os.path.join(self.new_image_dir, "manifest.json")
        json_manifest = self._dump_json(manifest, True)[0]

        self._write_json_metadata(json_manifest, manifest_file)

    def _generate_manifest_metadata(
        self,
        image_id,
        image_name,
        image_tag,
        old_image_manifest,
        layer_paths_to_move,
        layer_path_id=None,
    ):
        manifest = OrderedDict()
        manifest["Config"] = "%s.json" % image_id

        if image_name and image_tag:
            manifest["RepoTags"] = ["%s:%s" % (image_name, image_tag)]

        manifest["Layers"] = old_image_manifest["Layers"][: len(layer_paths_to_move)]

        if layer_path_id:
            manifest["Layers"].append("%s/layer.tar" % layer_path_id)

        return [manifest]

    def _read_json_file(self, json_file):
        """Helper function to read JSON file as OrderedDict"""

        self.log.debug(f"Reading '{json_file}' JSON file...")

        with open(json_file, "r") as f:
            return json.load(f, object_pairs_hook=OrderedDict)

    def _read_layer_paths(
        self, old_image_config, old_image_manifest, layers_to_move: List[str]
    ) -> Tuple[List[str], List[str]]:
        """
        In case of v2 format, layer id's are not the same as the id's
        used in the exported tar archive to name directories for layers.
        These id's can be found in the configuration files saved with
        the image - we need to read them.
        """

        # In manifest.json we do not have listed all layers
        # but only layers that do contain some data.
        current_manifest_layer = 0

        layer_paths_to_move = []
        layer_paths_to_squash = []

        # For tar input, we need to handle the layer structure differently
        if self.is_tar_input:
            # Use the layer list we built from tar metadata
            for i, layer_id in enumerate(self.old_image_layers):
                # Skip virtual/empty layers for path processing
                if layer_id.startswith("<missing-"):
                    continue

                # Check if this layer should be moved or squashed
                if len(layers_to_move) > i:
                    layer_paths_to_move.append(layer_id)
                else:
                    layer_paths_to_squash.append(layer_id)
        else:
            # Original logic for Docker daemon input
            # Iterate over image history, from base image to top layer
            for i, layer in enumerate(old_image_config["history"]):
                # If it's not an empty layer get the id
                # (directory name) where the layer's data is
                # stored
                if not layer.get("empty_layer", False):
                    # Under <25 layers look like
                    # 27f9b97654306a5389e8e48ba3486a11026d34055e1907672231cbd8e1380481/layer.tar
                    # while >=25 layers look like
                    # blobs/sha256/d6a7fc1fb44b63324d3fc67f016e1ef7ecc1a5ae6668ae3072d2e17230e3cfbc
                    if self.oci_format:
                        layer_id = old_image_manifest["Layers"][current_manifest_layer]
                    else:
                        layer_id = old_image_manifest["Layers"][
                            current_manifest_layer
                        ].rsplit("/")[0]

                    # Check if this layer should be moved or squashed
                    if len(layers_to_move) > i:
                        layer_paths_to_move.append(layer_id)
                    else:
                        layer_paths_to_squash.append(layer_id)

                    current_manifest_layer += 1

        return layer_paths_to_squash, layer_paths_to_move

    def _generate_chain_id(self, chain_ids, diff_ids, parent_chain_id):
        if parent_chain_id is None:
            return self._generate_chain_id(chain_ids, diff_ids[1:], diff_ids[0])

        chain_ids.append(parent_chain_id)

        if len(diff_ids) == 0:
            return parent_chain_id

        # This probably should not be hardcoded
        to_hash = "sha256:%s sha256:%s" % (parent_chain_id, diff_ids[0])
        digest = hashlib.sha256(str(to_hash).encode("utf8")).hexdigest()

        return self._generate_chain_id(chain_ids, diff_ids[1:], digest)

    def _generate_chain_ids(self, diff_ids):
        chain_ids = []

        self._generate_chain_id(chain_ids, diff_ids, None)

        return chain_ids

    def _generate_diff_ids(self):
        diff_ids = []

        for path in self.layer_paths_to_move:
            sha256 = self._compute_sha256(self._extract_tar_name(path))
            diff_ids.append(sha256)

        if self.layer_paths_to_squash:
            sha256 = self._compute_sha256(os.path.join(self.squashed_dir, "layer.tar"))
            diff_ids.append(sha256)

        return diff_ids

    def _compute_sha256(self, layer_tar):
        sha256 = hashlib.sha256()

        with open(layer_tar, "rb") as f:
            while True:
                # Read in 10MB chunks
                data = f.read(10485760)

                if not data:
                    break

                sha256.update(data)

        return sha256.hexdigest()

    def _generate_squashed_layer_path_id(self):
        """
        This function generates the id used to name the directory to
        store the squashed layer content in the archive.

        This mimics what Docker does here: https://github.com/docker/docker/blob/v1.10.0-rc1/image/v1/imagev1.go#L42
        To make it simpler we do reuse old image metadata and
        modify it to what it should look which means to be exact
        as https://github.com/docker/docker/blob/v1.10.0-rc1/image/v1/imagev1.go#L64
        """

        # Using OrderedDict, because order of JSON elements is important
        v1_metadata = OrderedDict(self.old_image_config)

        # Update image creation date
        v1_metadata["created"] = self.date

        # Remove unnecessary elements
        # Do not fail if key is not found
        for key in "history", "rootfs", "container":
            v1_metadata.pop(key, None)

        # Docker internally changes the order of keys between
        # exported metadata (why oh why?!). We need to add 'os'
        # element after 'layer_id'
        operating_system = v1_metadata.pop("os", None)

        # The 'layer_id' element is the chain_id of the
        # squashed layer
        v1_metadata["layer_id"] = "sha256:%s" % self.chain_ids[-1]

        # Add back 'os' element
        if operating_system:
            v1_metadata["os"] = operating_system

        # The 'parent' element is the name of the directory (inside the
        # exported tar archive) of the last layer that we move
        # (layer below squashed layer)

        if self.layer_paths_to_move:
            if self.layer_paths_to_squash:
                parent = self.layer_paths_to_move[-1]
            else:
                parent = self.layer_paths_to_move[0]

            v1_metadata["parent"] = "sha256:%s" % parent

        # The 'Image' element is the id of the layer from which we squash
        if self.squash_id:
            # Update image id, should be one layer below squashed layer
            v1_metadata["config"]["Image"] = self.squash_id
        else:
            v1_metadata["config"]["Image"] = ""

        # Get the sha256sum of the JSON exported metadata,
        # we do not care about the metadata anymore
        sha = self._dump_json(v1_metadata)[1]

        return sha

    def _generate_last_layer_metadata(self, layer_path_id, old_layer_path: Path):
        config_file = os.path.join(self.old_image_dir, old_layer_path)
        with open(config_file, "r") as f:
            config = json.load(f, object_pairs_hook=OrderedDict)

        config["created"] = self.date

        if self.squash_id:
            # Update image id, should be one layer below squashed layer
            config["config"]["Image"] = self.squash_id
        else:
            config["config"]["Image"] = ""

        # Update 'parent' - it should be path to the last layer to move
        if self.layer_paths_to_move:
            config["parent"] = self.layer_paths_to_move[-1]
        else:
            config.pop("parent", None)
        # Update 'id' - it should be the path to the layer
        config["id"] = layer_path_id
        config.pop("container", None)
        return config

    def _generate_image_metadata(self):
        # First - read old image config, we'll update it instead of
        # generating one from scratch
        metadata = OrderedDict(self.old_image_config)
        # Update image creation date
        metadata["created"] = self.date

        # Remove unnecessary or old fields
        metadata.pop("container", None)

        # Remove squashed layers from history
        metadata["history"] = metadata["history"][: len(self.layers_to_move)]
        # Remove diff_ids for squashed layers
        metadata["rootfs"]["diff_ids"] = metadata["rootfs"]["diff_ids"][
            : len(self.layer_paths_to_move)
        ]

        history = {"comment": self.comment, "created": self.date}

        if self.layer_paths_to_squash:
            # Add diff_ids for the squashed layer
            metadata["rootfs"]["diff_ids"].append("sha256:%s" % self.diff_ids[-1])
        else:
            history["empty_layer"] = True

        # Add new entry for squashed layer to history
        metadata["history"].append(history)

        if self.squash_id:
            # Update image id, should be one layer below squashed layer
            metadata["config"]["Image"] = self.squash_id
        else:
            metadata["config"]["Image"] = ""

        return metadata

    def _extract_tar_name(self, layer_id):
        """Get the path to a layer's tar file - handles both Docker daemon and tar input"""
        if self.is_tar_input:
            return self._get_tar_layer_path(layer_id)
        else:
            # Original logic for Docker daemon input
            if self.oci_format:
                return os.path.join(self.old_image_dir, layer_id)
            else:
                return os.path.join(self.old_image_dir, layer_id, "layer.tar")

    def _get_tar_layer_path(self, layer_id):
        """Get the path to a layer's tar file for tar input"""
        # Handle virtual/empty layers
        if layer_id.startswith("<missing-"):
            return None  # Virtual layer has no tar file

        if self.oci_format:
            # For OCI format, layers are in blobs/sha256/
            if layer_id.startswith("sha256:"):
                digest = layer_id.split(":", 1)[1]
            else:
                digest = layer_id
            return os.path.join(self.old_image_dir, "blobs", "sha256", digest)
        else:
            # For Docker format, layers are in directories
            if layer_id.startswith("sha256:"):
                layer_dir = layer_id.split(":", 1)[1]
            else:
                layer_dir = layer_id
            return os.path.join(self.old_image_dir, layer_dir, "layer.tar")

    def _dir_size(self, directory):
        """Calculate directory size"""
        size = 0
        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                if os.path.exists(file_path):
                    size += os.path.getsize(file_path)
        return size

    def _validate_number_of_layers(self, number_of_layers):
        """
        Makes sure that the specified number of layers to squash
        is a valid number
        """
        # Only positive numbers are correct
        if number_of_layers <= 0:
            raise SquashError(
                f"Number of layers to squash cannot be less or equal 0, provided: {number_of_layers}"
            )

        # Do not squash if provided number of layer to squash is bigger
        # than number of actual layers in the image
        if number_of_layers > len(self.old_image_layers):
            raise SquashError(
                f"Cannot squash {number_of_layers} layers, the {self.image} image contains only {len(self.old_image_layers)} layers"
            )

    def _parse_image_name(self, image):
        """
        Parses the provided image name and splits it in the
        name and tag part, if possible. If no tag is provided
        'latest' is used.
        """
        if ":" in image and "/" not in image.split(":")[-1]:
            image_tag = image.split(":")[-1]
            image_name = image[: -(len(image_tag) + 1)]
        else:
            image_tag = "latest"
            image_name = image

        return (image_name, image_tag)

    def _get_manifest(self):
        if os.path.exists(os.path.join(self.old_image_dir, "index.json")):
            # New OCI Archive format type
            self.oci_format = True
            # Not using index.json to extract manifest details as while the config
            # sha could be extracted via some indirection i.e.
            #
            # index.json:manifest/digest::sha256:<intermediary>
            # blobs/sha256/<intermediary>:config/digest::sha256:<config>
            #
            # Docker spec currently will always include a manifest.json so will standardise
            # on using that. Further we rely upon the original manifest format in order to write
            # it back.
            if os.path.exists(os.path.join(self.old_image_dir, "manifest.json")):
                return (
                    self._read_json_file(
                        os.path.join(self.old_image_dir, "manifest.json")
                    )
                )[0]
            else:
                raise SquashError("Unable to locate manifest.json")
        else:
            return (
                self._read_json_file(os.path.join(self.old_image_dir, "manifest.json"))
            )[0]
