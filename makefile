GOPATH = $(PWD)/build
export GOPATH
GOBIN = $(PWD)/build/bin
export GOBIN
URL = github.com/journeymidnight
REPO = nentropy
URLPATH = $(PWD)/build/src/$(URL)
GO?=go

all:
	@[ -d $(URLPATH) ] || mkdir -p $(URLPATH)
	@[ -d $(GOBIN) ] || mkdir -p $(GOBIN)
	@ln -nsf $(PWD) $(URLPATH)/$(REPO)
	${GO} build -o nentropy_monitor $(URLPATH)/$(REPO)/mon/*.go
	${GO} build -o nentropy_admin $(URLPATH)/$(REPO)/tools/admin.go
	${GO} build -o nentropy_osd $(URLPATH)/$(REPO)/osd/*.go
	mv nentropy_monitor $(PWD)/build/bin
	mv nentropy_admin $(PWD)/build/bin
	mv nentropy_osd $(PWD)/build/bin
test-osd:
	cd $(URLPATH)/$(REPO)/osd && go test -v
test-store:
	cd $(URLPATH)/$(REPO)/store && go test -v
test: test-osd  test-store
