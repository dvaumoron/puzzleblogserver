#!/usr/bin/env bash

go install

buildah from --name puzzleblogserver-working-container scratch
buildah copy puzzleblogserver-working-container $HOME/go/bin/puzzleblogserver /bin/puzzleblogserver
buildah config --env SERVICE_PORT=50051 puzzleblogserver-working-container
buildah config --port 50051 puzzleblogserver-working-container
buildah config --entrypoint '["/bin/puzzleblogserver"]' puzzleblogserver-working-container
buildah commit puzzleblogserver-working-container puzzleblogserver
buildah rm puzzleblogserver-working-container
