# CS 4254 Fall 2026 -- Project 3: Web Crawler (Fakebook)
#
# Build the executable required by section 10 of instructionsp3.txt.

PYTHON ?= python3
SRC     = webcrawler.py
TARGET  = webcrawler

.PHONY: all check run clean FORCE

all: $(TARGET)

# Refresh the executable on every build.
$(TARGET): $(SRC) FORCE
	@$(PYTHON) -m py_compile $(SRC)
	@rm -rf __pycache__
	@cp $(SRC) $(TARGET)
	@chmod +x $(TARGET)
	@echo "make: built ./$(TARGET) -- run ./$(TARGET) [username] [password]"

FORCE:

check:
	@$(PYTHON) -m py_compile $(SRC)
	@rm -rf __pycache__
	@echo "make: syntax OK"

# Development convenience: make run USER=x PASS=y
run: all
	@./$(TARGET) "$(USER)" "$(PASS)"

clean:
	@rm -f $(TARGET)
	@rm -rf __pycache__ *.pyc
	@echo "make: removed ./$(TARGET) and bytecode"
