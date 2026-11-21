PYTHON ?= python3
SRC     = webcrawler.py
TARGET  = webcrawler

.PHONY: all run clean

all: $(TARGET)

$(TARGET): $(SRC) 
	@$(PYTHON) -m py_compile $(SRC)
	@rm -rf __pycache__
	@cp $(SRC) $(TARGET)
	@chmod +x $(TARGET)
	@echo "run ./$(TARGET) [username] [password]"


run: all
	@./$(TARGET) "$(USER)" "$(PASS)"

clean:
	@rm -f $(TARGET)
	@rm -rf __pycache__ *.pyc
	@echo "removed ./$(TARGET)"
