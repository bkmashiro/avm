# Contributing to AVM

Thank you for your interest in contributing to AVM! 🎉

## Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/avm.git
   cd avm
   ```
3. Install in development mode:
   ```bash
   pip install -e ".[dev]"
   ```
4. Run tests:
   ```bash
   pytest
   ```

## Development Workflow

1. Create a branch for your feature:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes and write tests

3. Run tests and ensure they pass:
   ```bash
   pytest -v
   ```

4. Commit with a clear message:
   ```bash
   git commit -m "feat: add your feature description"
   ```

5. Push and create a Pull Request

## Commit Messages

We follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation only
- `refactor:` Code refactoring
- `test:` Adding tests
- `chore:` Maintenance

## Code Style

- Use type hints
- Write docstrings for public functions
- Keep functions focused and small
- Comments in English

## Project Structure

```
avm/
├── __init__.py      # Public API
├── core.py          # VFS core class
├── store.py         # SQLite storage
├── agent_memory.py  # Token-aware recall
├── fuse_mount.py    # FUSE filesystem
├── mcp_server.py    # MCP protocol server
├── handlers.py      # Pluggable handlers
├── permissions.py   # Linux-style permissions
└── providers/       # Data providers
```

## Adding a New Handler

```python
from avm import BaseHandler, register_handler

class MyHandler(BaseHandler):
    def read(self, path, context):
        # Your implementation
        return content
    
    def write(self, path, content, context):
        # Your implementation
        return True

register_handler('myhandler', MyHandler)
```

## Adding a New MCP Tool

Edit `avm/mcp_server.py`:

1. Add to `self.tools` in `__init__`
2. Add tool definition in `get_tool_definitions()`
3. Implement `_tool_yourname()` method

## Questions?

Open an issue or start a discussion. We're happy to help!
