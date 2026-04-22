# Fabric Ontology MCP Server

MCP server for full CRUD control of Ontology items in Microsoft Fabric.

## Features

- 🔍 **List Ontologies**: Browse all ontology items in your Fabric workspace
- 📊 **Get Ontology Details**: Retrieve detailed information about specific ontologies
- ✏️ **Create & Update**: Full CRUD operations for ontology management
- 🔐 **Secure Authentication**: Azure authentication support
- 🚀 **MCP Protocol**: Built on the Model Context Protocol for seamless integration

## Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd Ontology\ MCP\ Server

# Install dependencies
pip install -e .
```

## Requirements

- Python >= 3.11
- Microsoft Fabric workspace access
- Azure authentication credentials

## Usage

Run the MCP server:

```bash
fabric-ontology-mcp
```

## Project Structure

```
.
├── src/
│   ├── __init__.py
│   ├── __main__.py
│   ├── auth.py              # Azure authentication
│   ├── definition_utils.py  # Ontology definition utilities
│   ├── fabric_client.py     # Fabric API client
│   ├── models.py            # Data models
│   └── server.py            # MCP server implementation
├── add_bindings.py
├── check_timestamps.py
├── find_eh.py
├── find_schema.py
├── find_tables.py
├── main.py
├── push_ontology.py
├── server.py
├── test_mcp.py
├── verify.py
└── pyproject.toml
```

## Dependencies

- `httpx` - Async HTTP client
- `mcp[cli]` - Model Context Protocol with CLI support

## Development

### Setup Development Environment

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in development mode
pip install -e .
```

### Testing

```bash
python test_mcp.py
```

## License

[Add your license here]

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Author

[Your Name]
