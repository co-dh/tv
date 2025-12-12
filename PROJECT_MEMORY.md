# Table Viewer (tv)

## Project Description

A terminal-based table viewer built in Rust using polars, following the design patterns from ~/repo/qtv (Q Table Viewer).

## Core Features

### Data Stack
- Stack of tables with their display status (current row, column)
- Each view maintains its own cursor position and command history
- Navigate between different table views

### Command Executor
- Operates on the table stack
- Commands follow a pipe syntax: `delcol ns | filter date>2025-01-04 | sel name,addr | save a.parquet | pop`
- Records all commands to `commands.txt` for replay
- Transformations are tracked in history for transparency

### Keyboard Handler
- Converts key presses to commands
- Context-aware: pressing D on column "ns" sends `delcol ns` command
- Single-key shortcuts for common operations
- Interactive prompts for complex inputs

## Architecture

### State Management
- `TableState`: Display cursor (current row, column) and viewport (first visible row)
- `ViewState`: Complete table view including DataFrame, display state, and command history
- `StateStack`: Stack of views for navigating between table transformations

### Command System
- Command trait with `execute()` and `to_string()` methods
- Commands are self-describing for history recording
- Types: Navigation (cursor movement), Transformation (data changes), View (new views), IO (save/load)

### Rendering
- Terminal UI using crossterm
- Displays visible portion of table with proper alignment
- Highlights current cell
- Status bar shows filename, cursor position, and command history

## Command Reference

### MVP Commands

**Navigation**:
- Arrow keys: Move cursor
- Ctrl+D/U: Page down/up
- g/G: Go to top/bottom

**Data Transformation** (recorded in history):
- `D`: Delete current column
- `/`: Filter rows (prompts for expression)
- `s`: Select columns (prompts for column names)

**IO**:
- `L`: Load CSV or Parquet file
- `S`: Save to Parquet file

**Control**:
- `q`: Quit (or pop from stack)

## File Formats

- CSV: Universal format with type inference
- Parquet: Native polars format, efficient and fast

## History and Replay

All transformation commands are recorded to `commands.txt` in the current directory. Each command is saved as a human-readable string that can be edited and replayed in future sessions.

## Design Philosophy

- **Minimal and focused**: Start with core features, extend as needed
- **Command-based**: Every operation is a command for composability
- **Transparent**: Command history visible and editable
- **Efficient**: Only render visible data, use columnar storage
- **Keyboard-driven**: Fast navigation and operations without mouse

## Future Enhancements

- Command replay from history file
- Pipe syntax parsing for command chains
- More commands: sort, rename, frequency analysis, metadata view
- Stack operations: swap, duplicate, pop
- Fuzzy finder integration for interactive selection
- Search functionality
- Multiple file format support (JSON, Arrow)
