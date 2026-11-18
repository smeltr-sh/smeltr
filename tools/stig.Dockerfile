# DISA's General Purpose Operating System SRG, scanned against an image's filesystem: `just stig` writes the report
# to `stig/`. The profile is Chainguard's for Wolfi images (github.com/chainguard-dev/stigs, Apache-2.0), which
# marks each check a container cannot meet as not applicable, with its rationale.
#
# The image's files are copied into an Ubuntu stage, which carries OpenSCAP, and scanned there as a chroot. The copy
# keeps owners and modes, which the file-permission rules read, and the image itself gains nothing. The content is
# pinned by release and checksum; a newer release means a newer pin.
ARG IMAGE=smeltr:latest
FROM ${IMAGE} AS target

FROM ubuntu:24.04 AS scan
RUN apt-get update \
    && apt-get install -y --no-install-recommends openscap-scanner openscap-utils \
    && rm -rf /var/lib/apt/lists/*
# chainguard-dev/stigs v3.2.17: the GPOS SRG V3R2 profile.
ADD --checksum=sha256:6f7711b7759c3f9fd1aac4b8ea275075fd9d8810b7fccdeeffe5825a4ceef41e \
    https://raw.githubusercontent.com/chainguard-dev/stigs/5653bbf056a9074d1280ada4995e4dbff974cc75/gpos/xml/scap/ssg/content/ssg-chainguard-gpos-ds.xml /gpos-ds.xml
COPY --from=target / /target
# The profile walks /usr/lib on local filesystems only, and counts the overlay a build stage runs on as remote, so it
# would find no files there and fail the library-ownership rules.
RUN sed -i 's/recurse_file_system="local"/recurse_file_system="all"/' /gpos-ds.xml
# oscap exits 2 when a rule fails, which is a finding for the report, not a failed scan.
RUN oscap-chroot /target xccdf eval --profile xccdf_basic_profile_.check --report /report.html \
      /gpos-ds.xml > /results.txt; \
    s=$?; grep -c '^Result.*fail' /results.txt; test $s -eq 0 -o $s -eq 2

FROM scratch
COPY --from=scan /report.html /results.txt /
