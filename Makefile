test:
	docker build . -t lifs_go_test --target test && \
	docker run --privileged lifs_go_test
