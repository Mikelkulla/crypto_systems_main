import os
import sys
from pathlib import Path

def print_directory_tree(directory, prefix="", level=0, max_depth=None, ignore=None):
    """
    Print a directory tree structure starting from the given directory and display file contents.
    
    Args:
        directory: Path to the directory to display
        prefix: Prefix string for formatting (used in recursion)
        level: Current depth level in the tree
        max_depth: Maximum depth to explore (None for unlimited)
        ignore: List of directory/file names to ignore
    """
    if max_depth is not None and level > max_depth:
        return
        
    directory = Path(directory)
    
    # Default ignore list
    if ignore is None:
        ignore = ['__pycache__', '.git', 'node_modules', '.venv', 'venv']

    try:
        if not directory.exists():
            print(f"Error: Directory '{directory}' does not exist")
            return
        if not directory.is_dir():
            print(f"Error: '{directory}' is not a directory")
            return

        # Get all entries in the directory
        entries = sorted(directory.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
        
        for index, entry in enumerate(entries):
            if entry.name in ignore:
                continue
                
            try:
                # Determine if this is the last entry at this level
                is_last = index == len(entries) - 1
                
                # Create the appropriate prefix for this level
                current_prefix = prefix + ("└── " if is_last else "├── ")
                
                # Print the current entry
                print(f"{prefix}{current_prefix}{entry.name}{'/' if entry.is_dir() else ''}")
                
                # If it's a file, print its contents
                if entry.is_file():
                    try:
                        with open(entry, 'r', encoding='utf-8') as file:
                            content = file.read()
                            if content.strip():  # Only print non-empty files
                                # Split content into lines and add indentation
                                content_lines = content.splitlines()
                                file_prefix = prefix + ("    " if is_last else "│   ")
                                print(f"{file_prefix}└── [Content]")
                                for line in content_lines:
                                    print(f"{file_prefix}    {line}")
                            else:
                                print(f"{prefix}    └── [Empty File]")
                    except UnicodeDecodeError:
                        print(f"{prefix}    └── [Binary or Non-UTF-8 File]")
                    except PermissionError:
                        print(f"{prefix}    └── [Permission Denied for File Content]")
                    except OSError as e:
                        print(f"{prefix}    └── [Error Reading File: {str(e)}]")
                
                # If it's a directory, recurse into it
                if entry.is_dir():
                    # Create the prefix for the next level
                    next_prefix = prefix + ("    " if is_last else "│   ")
                    print_directory_tree(
                        entry,
                        next_prefix,
                        level + 1,
                        max_depth,
                        ignore
                    )
            except PermissionError:
                print(f"{prefix}{current_prefix}{entry.name} [Permission Denied]")
            except OSError as e:
                print(f"{prefix}{current_prefix}{entry.name} [Error: {str(e)}]")
                
    except PermissionError:
        print(f"Error: Permission denied accessing '{directory}'")
    except OSError as e:
        print(f"Error: Failed to access '{directory}' - {str(e)}")

def main():
    # Get directory from command line argument or use current directory
    directory = Path.cwd()
    if len(sys.argv) > 1:
        directory = Path(sys.argv[1])
    
    # Optional: Add max_depth and ignore list as command line arguments
    max_depth = None
    ignore = ['__pycache__', '.git', 'node_modules', '.venv', 'venv']
    
    try:
        print(f"Directory structure for: {directory}")
        print_directory_tree(directory, max_depth=max_depth, ignore=ignore)
    except Exception as e:
        print(f"Error: An unexpected error occurred - {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()