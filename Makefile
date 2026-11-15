.PHONY: test desktop-build desktop-test desktop-frontend desktop-run desktop-frontend-dev

test:
	go test ./...

desktop-frontend:
	cd desktop/frontend && npm run typecheck && npm run build

desktop-build:
	cd desktop && DESKTOP_UI=next ./build.sh

desktop-test:
	cd desktop && go test ./...
	cd desktop/frontend && npm run typecheck && npm test

desktop-run:
	cd desktop && DESKTOP_UI=next ./build.sh run

desktop-frontend-dev:
	cd desktop/frontend && npm run dev
