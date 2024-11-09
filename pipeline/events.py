import os
import requests
import solcx
import tempfile
import urllib.parse

from enum import StrEnum
from typing import Dict, Any
from typing import List, Optional, Union
from dataclasses import dataclass
from hypermanager.events import EventConfig
from hypermanager.schema import COMMON_TRANSACTION_MAPPING, COMMON_BLOCK_MAPPING
from hypersync import ColumnMapping


@dataclass
class ContractInput:
    """
    Represents an input parameter for a contract function or event.

    Attributes:
        name (str): Name of the input parameter
        type (str): Data type of the input parameter
        indexed (bool, optional): Whether the input is indexed (for events)
    """
    name: str = ''
    type: str = ''
    indexed: bool = False


@dataclass
class ContractEvent:
    """
    Represents a contract event with its details.

    Attributes:
        name (str): Name of the event
        signature (str): Full signature of the event
        inputs (List[ContractInput]): Input parameters of the event
    """
    name: str
    signature: str
    inputs: List[ContractInput]


@dataclass
class ContractFunction:
    """
    Represents a contract function with its details.

    Attributes:
        name (str): Name of the function
        signature (str): Full signature of the function
        inputs (List[ContractInput]): Input parameters of the function
        outputs (List[ContractInput]): Output parameters of the function
        stateMutability (str): State mutability of the function
    """
    name: str
    signature: str
    inputs: List[ContractInput]
    outputs: List[ContractInput]
    stateMutability: str


def extract_contract_details(
    contract_source: str,
    version: str = '0.8.26'
) -> Optional[Dict[str, List[Union[ContractEvent, ContractFunction]]]]:
    """
    Extract metadata (events and functions) from a Solidity contract.

    Args:
        contract_source (str): URL or local file path of the Solidity contract
        version (str, optional): Solidity compiler version. Defaults to '0.8.26'

    Returns:
        Optional dictionary with 'events' and 'functions' or None if extraction fails

    Raises:
        FileNotFoundError: If local contract file does not exist
        requests.RequestException: If contract URL cannot be downloaded
    """
    try:
        # Determine if source is a URL or local file path
        if contract_source.startswith(('http://', 'https://')):
            # URL handling
            response = requests.get(contract_source)
            response.raise_for_status()

            # Use URL-based filename
            parsed_url = urllib.parse.urlparse(contract_source)
            filename = os.path.basename(parsed_url.path) or 'contract.sol'

            # Create temporary file
            with tempfile.NamedTemporaryFile(mode='w', prefix=filename, suffix='.sol', delete=False) as temp_file:
                temp_file.write(response.text)
                temp_file_path = temp_file.name
        else:
            # Local file path handling
            if not os.path.exists(contract_source):
                raise FileNotFoundError(
                    f"Contract file not found: {contract_source}")

            temp_file_path = contract_source

        try:
            # Install and set Solidity compiler version
            if version not in solcx.get_installed_solc_versions():
                solcx.install_solc(version)
            solcx.set_solc_version(version)

            # Compile contract
            compiled_sol = solcx.compile_files(
                [temp_file_path],
                output_values=['abi', 'bin']
            )

            # Process contract details
            # Take the first (and typically only) contract from the compilation
            contract_name = list(compiled_sol.keys())[0]
            abi = compiled_sol[contract_name]['abi']

            # Extract Events
            events = [
                ContractEvent(
                    name=event['name'],
                    signature=f"{event['name']}({', '.join([f'{"indexed " if inp.get("indexed", False) else ""}{
                        inp["type"]} {inp.get("name", "")}'.strip() for inp in event.get("inputs", [])])})",
                    inputs=[
                        ContractInput(
                            name=inp.get('name', ''),
                            type=inp['type'],
                            indexed=inp.get('indexed', False)
                        ) for inp in event.get('inputs', [])
                    ]
                )
                for event in abi if event['type'] == 'event'
            ]

            # Extract Functions
            functions = [
                ContractFunction(
                    name=func['name'],
                    signature=f"{func['name']}({','.join(
                        inp['type'] for inp in func.get('inputs', []))})",
                    inputs=[
                        ContractInput(
                            name=inp.get('name', ''),
                            type=inp['type']
                        ) for inp in func.get('inputs', [])
                    ],
                    outputs=[
                        ContractInput(
                            name=out.get('name', ''),
                            type=out['type']
                        ) for out in func.get('outputs', [])
                    ],
                    stateMutability=func.get('stateMutability', '')
                )
                for func in abi if func['type'] == 'function'
            ]

            # Return a clean dictionary with just events and functions
            return {
                'events': events,
                'functions': functions
            }

        finally:
            # Clean up the temporary file
            os.unlink(temp_file_path)

    except requests.RequestException as e:
        print(f"Error downloading contract: {e}")
        return None
    except Exception as e:
        print(f"Compilation error: {e}")
        return None


class DataType(StrEnum):
    """Hypersync datatypes supported for mapping."""
    UINT64 = "uint64"
    UINT32 = "uint32"
    INT64 = "int64"
    INT32 = "int32"
    FLOAT32 = "float32"
    FLOAT64 = "float64"


def solidity_to_datatype(solidity_type: str) -> DataType:
    """
    Convert Solidity type to corresponding DataType.

    Args:
        solidity_type (str): Solidity type string

    Returns:
        DataType: Mapped data type
    """
    # Unsigned integer mappings
    if solidity_type.startswith('uint'):
        # Extract bit size
        try:
            bits = int(solidity_type[4:])
            if bits <= 32:
                return DataType.UINT32
            return DataType.UINT64
        except ValueError:
            return DataType.UINT64  # default to UINT64 if parsing fails

    # Signed integer mappings
    if solidity_type.startswith('int'):
        try:
            bits = int(solidity_type[3:])
            if bits <= 32:
                return DataType.INT32
            return DataType.INT64
        except ValueError:
            return DataType.INT64  # default to INT64 if parsing fails

    # Floating point (not directly supported in Solidity)
    # This is a placeholder and might need more sophisticated handling
    if solidity_type == 'fixed' or solidity_type.startswith('fixed'):
        return DataType.FLOAT64

    # Default case for types not explicitly mapped
    return DataType.UINT64  # Conservative default


def generate_event_config(event_details: ContractEvent) -> EventConfig:
    """
    Generate event configuration based on extracted event details.

    Args:
        event_details (ContractEvent): Event details from contract extraction

    Returns:
        Dict: Generated event configuration
    """
    # Initialize decoded_log dictionary
    decoded_log = {}

    # Map inputs to DataTypes
    for input_param in event_details.inputs:
        # Skip indexed parameters (usually addresses)
        if input_param.indexed:
            continue

        # Map parameter name and type
        param_name = input_param.name
        param_type = input_param.type

        # Skip if no name or type
        if not param_name or not param_type:
            continue

        # Convert Solidity type to DataType
        data_type = solidity_to_datatype(param_type)

        decoded_log[param_name] = data_type

    # Create a basic event configuration
    event_config = EventConfig(
        name=event_details.name,
        signature=event_details.signature,
        column_mapping=ColumnMapping(
            decoded_log=decoded_log,
            transaction=COMMON_TRANSACTION_MAPPING,
            block=COMMON_BLOCK_MAPPING,
        ),
    )

    return event_config


def generate_all_event_configs(contract_details) -> list[EventConfig]:
    """
    Generate event configurations for all events in a contract.

    Args:
        contract_details (Dict): Contract details from extraction

    Returns:
        Dict: Mapping of event names to their configurations
    """
    event_configs = []

    for event in contract_details.get('events', []):
        # Construct the event signature with 'indexed' after the type
        inputs = ', '.join(f"{param['type']} {'indexed' if param.get('indexed', False) else ''} {param['name']}".strip() for param in event['inputs'])
        # Remove any extra spaces
        inputs = ' '.join(inputs.split())
        signature = f"{event['name']}({inputs})"
        event_config: ContractEvent = generate_event_config(event)
        event_configs.append(event_config)

    return event_configs
