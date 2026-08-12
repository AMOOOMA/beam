from collections.abc import Iterable as _Iterable
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper

DESCRIPTOR: _descriptor.FileDescriptor

class Sdk(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    SDK_UNSPECIFIED: _ClassVar[Sdk]
    SDK_JAVA: _ClassVar[Sdk]
    SDK_GO: _ClassVar[Sdk]
    SDK_PYTHON: _ClassVar[Sdk]
    SDK_SCIO: _ClassVar[Sdk]

class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    STATUS_UNSPECIFIED: _ClassVar[Status]
    STATUS_VALIDATING: _ClassVar[Status]
    STATUS_VALIDATION_ERROR: _ClassVar[Status]
    STATUS_PREPARING: _ClassVar[Status]
    STATUS_PREPARATION_ERROR: _ClassVar[Status]
    STATUS_COMPILING: _ClassVar[Status]
    STATUS_COMPILE_ERROR: _ClassVar[Status]
    STATUS_EXECUTING: _ClassVar[Status]
    STATUS_FINISHED: _ClassVar[Status]
    STATUS_RUN_ERROR: _ClassVar[Status]
    STATUS_ERROR: _ClassVar[Status]
    STATUS_RUN_TIMEOUT: _ClassVar[Status]
    STATUS_CANCELED: _ClassVar[Status]

class PrecompiledObjectType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    PRECOMPILED_OBJECT_TYPE_UNSPECIFIED: _ClassVar[PrecompiledObjectType]
    PRECOMPILED_OBJECT_TYPE_EXAMPLE: _ClassVar[PrecompiledObjectType]
    PRECOMPILED_OBJECT_TYPE_KATA: _ClassVar[PrecompiledObjectType]
    PRECOMPILED_OBJECT_TYPE_UNIT_TEST: _ClassVar[PrecompiledObjectType]

class Complexity(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    COMPLEXITY_UNSPECIFIED: _ClassVar[Complexity]
    COMPLEXITY_BASIC: _ClassVar[Complexity]
    COMPLEXITY_MEDIUM: _ClassVar[Complexity]
    COMPLEXITY_ADVANCED: _ClassVar[Complexity]

class EmulatorType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    EMULATOR_TYPE_UNSPECIFIED: _ClassVar[EmulatorType]
    EMULATOR_TYPE_KAFKA: _ClassVar[EmulatorType]
SDK_UNSPECIFIED: Sdk
SDK_JAVA: Sdk
SDK_GO: Sdk
SDK_PYTHON: Sdk
SDK_SCIO: Sdk
STATUS_UNSPECIFIED: Status
STATUS_VALIDATING: Status
STATUS_VALIDATION_ERROR: Status
STATUS_PREPARING: Status
STATUS_PREPARATION_ERROR: Status
STATUS_COMPILING: Status
STATUS_COMPILE_ERROR: Status
STATUS_EXECUTING: Status
STATUS_FINISHED: Status
STATUS_RUN_ERROR: Status
STATUS_ERROR: Status
STATUS_RUN_TIMEOUT: Status
STATUS_CANCELED: Status
PRECOMPILED_OBJECT_TYPE_UNSPECIFIED: PrecompiledObjectType
PRECOMPILED_OBJECT_TYPE_EXAMPLE: PrecompiledObjectType
PRECOMPILED_OBJECT_TYPE_KATA: PrecompiledObjectType
PRECOMPILED_OBJECT_TYPE_UNIT_TEST: PrecompiledObjectType
COMPLEXITY_UNSPECIFIED: Complexity
COMPLEXITY_BASIC: Complexity
COMPLEXITY_MEDIUM: Complexity
COMPLEXITY_ADVANCED: Complexity
EMULATOR_TYPE_UNSPECIFIED: EmulatorType
EMULATOR_TYPE_KAFKA: EmulatorType

class Dataset(_message.Message):
    __slots__ = ["dataset_path", "options", "type"]
    class OptionsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: str | None = ..., value: str | None = ...) -> None: ...
    TYPE_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    DATASET_PATH_FIELD_NUMBER: _ClassVar[int]
    type: EmulatorType
    options: _containers.ScalarMap[str, str]
    dataset_path: str
    def __init__(self, type: EmulatorType | str | None = ..., options: _Mapping[str, str] | None = ..., dataset_path: str | None = ...) -> None: ...

class RunCodeRequest(_message.Message):
    __slots__ = ["code", "datasets", "files", "pipeline_options", "sdk"]
    CODE_FIELD_NUMBER: _ClassVar[int]
    SDK_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    DATASETS_FIELD_NUMBER: _ClassVar[int]
    FILES_FIELD_NUMBER: _ClassVar[int]
    code: str
    sdk: Sdk
    pipeline_options: str
    datasets: _containers.RepeatedCompositeFieldContainer[Dataset]
    files: _containers.RepeatedCompositeFieldContainer[SnippetFile]
    def __init__(self, code: str | None = ..., sdk: Sdk | str | None = ..., pipeline_options: str | None = ..., datasets: _Iterable[Dataset | _Mapping] | None = ..., files: _Iterable[SnippetFile | _Mapping] | None = ...) -> None: ...

class RunCodeResponse(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: str | None = ...) -> None: ...

class CheckStatusRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: str | None = ...) -> None: ...

class CheckStatusResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: Status
    def __init__(self, status: Status | str | None = ...) -> None: ...

class GetValidationOutputRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: str | None = ...) -> None: ...

class GetValidationOutputResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: str | None = ...) -> None: ...

class GetPreparationOutputRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: str | None = ...) -> None: ...

class GetPreparationOutputResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: str | None = ...) -> None: ...

class GetCompileOutputRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: str | None = ...) -> None: ...

class GetCompileOutputResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: str | None = ...) -> None: ...

class GetRunOutputRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: str | None = ...) -> None: ...

class GetRunOutputResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: str | None = ...) -> None: ...

class GetRunErrorRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: str | None = ...) -> None: ...

class GetRunErrorResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: str | None = ...) -> None: ...

class GetLogsRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: str | None = ...) -> None: ...

class GetLogsResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: str | None = ...) -> None: ...

class GetGraphRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: str | None = ...) -> None: ...

class GetGraphResponse(_message.Message):
    __slots__ = ["graph"]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    graph: str
    def __init__(self, graph: str | None = ...) -> None: ...

class CancelRequest(_message.Message):
    __slots__ = ["pipeline_uuid"]
    PIPELINE_UUID_FIELD_NUMBER: _ClassVar[int]
    pipeline_uuid: str
    def __init__(self, pipeline_uuid: str | None = ...) -> None: ...

class CancelResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class PrecompiledObject(_message.Message):
    __slots__ = ["always_run", "cloud_path", "complexity", "context_line", "datasets", "default_example", "description", "link", "multifile", "name", "never_run", "pipeline_options", "sdk", "tags", "type", "url_notebook", "url_vcs"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    LINK_FIELD_NUMBER: _ClassVar[int]
    MULTIFILE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_LINE_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_EXAMPLE_FIELD_NUMBER: _ClassVar[int]
    SDK_FIELD_NUMBER: _ClassVar[int]
    COMPLEXITY_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    DATASETS_FIELD_NUMBER: _ClassVar[int]
    URL_VCS_FIELD_NUMBER: _ClassVar[int]
    URL_NOTEBOOK_FIELD_NUMBER: _ClassVar[int]
    ALWAYS_RUN_FIELD_NUMBER: _ClassVar[int]
    NEVER_RUN_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    name: str
    description: str
    type: PrecompiledObjectType
    pipeline_options: str
    link: str
    multifile: bool
    context_line: int
    default_example: bool
    sdk: Sdk
    complexity: Complexity
    tags: _containers.RepeatedScalarFieldContainer[str]
    datasets: _containers.RepeatedCompositeFieldContainer[Dataset]
    url_vcs: str
    url_notebook: str
    always_run: bool
    never_run: bool
    def __init__(self, cloud_path: str | None = ..., name: str | None = ..., description: str | None = ..., type: PrecompiledObjectType | str | None = ..., pipeline_options: str | None = ..., link: str | None = ..., multifile: bool = ..., context_line: int | None = ..., default_example: bool = ..., sdk: Sdk | str | None = ..., complexity: Complexity | str | None = ..., tags: _Iterable[str] | None = ..., datasets: _Iterable[Dataset | _Mapping] | None = ..., url_vcs: str | None = ..., url_notebook: str | None = ..., always_run: bool = ..., never_run: bool = ...) -> None: ...

class Categories(_message.Message):
    __slots__ = ["categories", "sdk"]
    class Category(_message.Message):
        __slots__ = ["category_name", "precompiled_objects"]
        CATEGORY_NAME_FIELD_NUMBER: _ClassVar[int]
        PRECOMPILED_OBJECTS_FIELD_NUMBER: _ClassVar[int]
        category_name: str
        precompiled_objects: _containers.RepeatedCompositeFieldContainer[PrecompiledObject]
        def __init__(self, category_name: str | None = ..., precompiled_objects: _Iterable[PrecompiledObject | _Mapping] | None = ...) -> None: ...
    SDK_FIELD_NUMBER: _ClassVar[int]
    CATEGORIES_FIELD_NUMBER: _ClassVar[int]
    sdk: Sdk
    categories: _containers.RepeatedCompositeFieldContainer[Categories.Category]
    def __init__(self, sdk: Sdk | str | None = ..., categories: _Iterable[Categories.Category | _Mapping] | None = ...) -> None: ...

class GetPrecompiledObjectsRequest(_message.Message):
    __slots__ = ["category", "sdk"]
    SDK_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    sdk: Sdk
    category: str
    def __init__(self, sdk: Sdk | str | None = ..., category: str | None = ...) -> None: ...

class GetPrecompiledObjectRequest(_message.Message):
    __slots__ = ["cloud_path"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    def __init__(self, cloud_path: str | None = ...) -> None: ...

class GetPrecompiledObjectCodeRequest(_message.Message):
    __slots__ = ["cloud_path"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    def __init__(self, cloud_path: str | None = ...) -> None: ...

class GetPrecompiledObjectOutputRequest(_message.Message):
    __slots__ = ["cloud_path"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    def __init__(self, cloud_path: str | None = ...) -> None: ...

class GetPrecompiledObjectLogsRequest(_message.Message):
    __slots__ = ["cloud_path"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    def __init__(self, cloud_path: str | None = ...) -> None: ...

class GetPrecompiledObjectGraphRequest(_message.Message):
    __slots__ = ["cloud_path"]
    CLOUD_PATH_FIELD_NUMBER: _ClassVar[int]
    cloud_path: str
    def __init__(self, cloud_path: str | None = ...) -> None: ...

class GetDefaultPrecompiledObjectRequest(_message.Message):
    __slots__ = ["sdk"]
    SDK_FIELD_NUMBER: _ClassVar[int]
    sdk: Sdk
    def __init__(self, sdk: Sdk | str | None = ...) -> None: ...

class GetPrecompiledObjectsResponse(_message.Message):
    __slots__ = ["sdk_categories"]
    SDK_CATEGORIES_FIELD_NUMBER: _ClassVar[int]
    sdk_categories: _containers.RepeatedCompositeFieldContainer[Categories]
    def __init__(self, sdk_categories: _Iterable[Categories | _Mapping] | None = ...) -> None: ...

class GetPrecompiledObjectResponse(_message.Message):
    __slots__ = ["precompiled_object"]
    PRECOMPILED_OBJECT_FIELD_NUMBER: _ClassVar[int]
    precompiled_object: PrecompiledObject
    def __init__(self, precompiled_object: PrecompiledObject | _Mapping | None = ...) -> None: ...

class GetPrecompiledObjectCodeResponse(_message.Message):
    __slots__ = ["code", "files"]
    CODE_FIELD_NUMBER: _ClassVar[int]
    FILES_FIELD_NUMBER: _ClassVar[int]
    code: str
    files: _containers.RepeatedCompositeFieldContainer[SnippetFile]
    def __init__(self, code: str | None = ..., files: _Iterable[SnippetFile | _Mapping] | None = ...) -> None: ...

class GetPrecompiledObjectOutputResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: str | None = ...) -> None: ...

class GetPrecompiledObjectLogsResponse(_message.Message):
    __slots__ = ["output"]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    output: str
    def __init__(self, output: str | None = ...) -> None: ...

class GetPrecompiledObjectGraphResponse(_message.Message):
    __slots__ = ["graph"]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    graph: str
    def __init__(self, graph: str | None = ...) -> None: ...

class GetDefaultPrecompiledObjectResponse(_message.Message):
    __slots__ = ["precompiled_object"]
    PRECOMPILED_OBJECT_FIELD_NUMBER: _ClassVar[int]
    precompiled_object: PrecompiledObject
    def __init__(self, precompiled_object: PrecompiledObject | _Mapping | None = ...) -> None: ...

class SnippetFile(_message.Message):
    __slots__ = ["content", "is_main", "name"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    IS_MAIN_FIELD_NUMBER: _ClassVar[int]
    name: str
    content: str
    is_main: bool
    def __init__(self, name: str | None = ..., content: str | None = ..., is_main: bool = ...) -> None: ...

class SaveSnippetRequest(_message.Message):
    __slots__ = ["complexity", "files", "persistence_key", "pipeline_options", "sdk"]
    FILES_FIELD_NUMBER: _ClassVar[int]
    SDK_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    COMPLEXITY_FIELD_NUMBER: _ClassVar[int]
    PERSISTENCE_KEY_FIELD_NUMBER: _ClassVar[int]
    files: _containers.RepeatedCompositeFieldContainer[SnippetFile]
    sdk: Sdk
    pipeline_options: str
    complexity: Complexity
    persistence_key: str
    def __init__(self, files: _Iterable[SnippetFile | _Mapping] | None = ..., sdk: Sdk | str | None = ..., pipeline_options: str | None = ..., complexity: Complexity | str | None = ..., persistence_key: str | None = ...) -> None: ...

class SaveSnippetResponse(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: str | None = ...) -> None: ...

class GetSnippetRequest(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: str | None = ...) -> None: ...

class GetSnippetResponse(_message.Message):
    __slots__ = ["complexity", "files", "pipeline_options", "sdk"]
    FILES_FIELD_NUMBER: _ClassVar[int]
    SDK_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    COMPLEXITY_FIELD_NUMBER: _ClassVar[int]
    files: _containers.RepeatedCompositeFieldContainer[SnippetFile]
    sdk: Sdk
    pipeline_options: str
    complexity: Complexity
    def __init__(self, files: _Iterable[SnippetFile | _Mapping] | None = ..., sdk: Sdk | str | None = ..., pipeline_options: str | None = ..., complexity: Complexity | str | None = ...) -> None: ...

class GetMetadataRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class GetMetadataResponse(_message.Message):
    __slots__ = ["beam_sdk_version", "build_commit_hash", "build_commit_timestamp_seconds_since_epoch", "runner_sdk"]
    RUNNER_SDK_FIELD_NUMBER: _ClassVar[int]
    BUILD_COMMIT_HASH_FIELD_NUMBER: _ClassVar[int]
    BUILD_COMMIT_TIMESTAMP_SECONDS_SINCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    BEAM_SDK_VERSION_FIELD_NUMBER: _ClassVar[int]
    runner_sdk: str
    build_commit_hash: str
    build_commit_timestamp_seconds_since_epoch: int
    beam_sdk_version: str
    def __init__(self, runner_sdk: str | None = ..., build_commit_hash: str | None = ..., build_commit_timestamp_seconds_since_epoch: int | None = ..., beam_sdk_version: str | None = ...) -> None: ...
