run:
	mkdir build
	cd build && cmake .. && make

exec:
	cd build && ./TransactionManager
	
clean:
	rm -R build