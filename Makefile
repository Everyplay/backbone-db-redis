REPORTER = spec

test:
	@./node_modules/.bin/mocha --ui tdd --reporter $(REPORTER) $(T) $(TESTS)

.PHONY: test
