ENV = test
REPORTER = spec
BIN = node_modules/.bin

# Use grep to run only tests with keywords:
# make test GREP=events
ifeq ($(GREP), )
	GREP_CMND =
else
 	GREP_CMND = --grep $(GREP)
endif

MOCHA-OPTS = --reporter $(REPORTER) \
		--require chai \
		--ui bdd \
		--recursive \
		--colors

test:
	@NODE_ENV=$(ENV) $(BIN)/mocha \
		$(MOCHA-OPTS) \
		$(GREP_CMND)
.PHONY: test

lint:
	npm run lint
.PHONY: lint

## Coverage:

test-coverage:
	@NODE_ENV=test $(BIN)/istanbul cover $(BIN)/_mocha -- $(MOCHA-OPTS)
.PHONY: test-coverage

check-coverage: test-coverage
	@$(BIN)/istanbul check-coverage --function 80 --branch 80 --statement 80 --lines 92
.PHONY: check-coverage
