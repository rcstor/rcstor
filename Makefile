.PHONY: rcstor rcdir rchttp rccli install tar bin remote .changeos

install : bin
	cp bin/rcstor /usr/local/bin/rcstor
	cp bin/rcdir /usr/local/bin/rcdir
	cp bin/rchttp /usr/local/bin/rchttp
	cp bin/rccli /usr/local/bin/rccli

bin: rcstor rcdir rchttp rccli

rcstor :
	go build -o bin/rcstor ./rcstor

rcdir :
	go build -o bin/rcdir ./rcdir

rccli :
	go build -o bin/rccli ./rccli

rchttp :
	go build -o bin/rchttp ./rchttp
