# CS 4254 Fall 2026 -- Project 3: Web Crawler (Fakebook)
#
# Python submission: there is nothing to compile, so `make` only needs to
# produce an executable named `webcrawler` in this directory (see
# instructionsp3.txt section 10). We keep the source as webcrawler.py and
# copy it to ./webcrawler at build time -- copied rather than symlinked so the
# tarball unpacks correctly on any rlogin machine. A syntax check runs too, so
# a typo fails `make` instead of 15 minutes into the grading run.

PYTHON ?= python3
SRC     = webcrawler.py
TARGET  = webcrawler

.PHONY: all check run clean FORCE

# Default goal: the grader runs plain `make` and then `./webcrawler`.
all: $(TARGET)

# FORCE makes this rule run every time, so `make` is always idempotent and
# never leaves a stale copy behind after you edit the source.
$(TARGET): $(SRC) FORCE
	@$(PYTHON) -m py_compile $(SRC)
	@rm -rf __pycache__
	@cp $(SRC) $(TARGET)
	@chmod +x $(TARGET)
	@echo "make: built ./$(TARGET) -- run ./$(TARGET) [username] [password]"

FORCE:

# Optional sanity target: make check
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
