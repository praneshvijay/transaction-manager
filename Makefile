run:
	g++ transaction_manager.cpp -o tm
	./tm

clean:
	rm -f tm