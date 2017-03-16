DOC_DIR="./docs"
TMP_DOC_DIR=$(DOC_DIR).tmp
COMMIT?=HEAD
BRANCH?=master
SHORT_COMMIT=$(shell git rev-parse $(COMMIT) | cut -c 1-8)

docs: $(DOC_DIR)
	git add $(DOC_DIR)
	git commit -m "update documentation ($(SHORT_COMMIT))."

$(DOC_DIR): $(TMP_DOC_DIR)
	git checkout $(BRANCH)
	mv $(DOC_DIR).tmp $(DOC_DIR)

$(TMP_DOC_DIR):
	git checkout $(COMMIT)
	rm -rf $(DOC_DIR)
	lein codox
	mv $(DOC_DIR) $(DOC_DIR).tmp
