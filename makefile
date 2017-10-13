GOPATH = $(PWD)/build
export GOPATH
GOBIN = $(PWD)/build/bin
export GOBIN
URL = github.com/journeymidnight
REPO = nentropy
URLPATH = $(PWD)/build/src/$(URL)

all:
	@[ -d $(URLPATH) ] || mkdir -p $(URLPATH)
	@[ -d $(GOBIN) ] || mkdir -p $(GOBIN)
	@ln -nsf $(PWD) $(URLPATH)/$(REPO)
	go build -o nentropy_monitor $(URLPATH)/$(REPO)/mon/*.go
	go build -o nentropy_admin $(URLPATH)/$(REPO)/tools/admin.go
	go build -o nentropy_osd $(URLPATH)/$(REPO)/osd/cmd/osd_daemon.go
	mv nentropy_monitor $(PWD)/build/bin
	mv nentropy_admin $(PWD)/build/bin
	mv nentropy_osd $(PWD)/build/bin
