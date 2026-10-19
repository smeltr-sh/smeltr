# The RHEL 9 STIG scanned against an image's filesystem: `just stig` writes the report to `stig/`.
#
# The image's files are copied into an Ubuntu stage, which carries OpenSCAP and the ComplianceAsCode content, and
# scanned there as a chroot. The copy keeps owners and modes, which the file-permission rules read, and the image
# itself gains nothing. The content is pinned by version and checksum; a newer STIG release means a newer one.
ARG IMAGE=smeltr:latest
FROM ${IMAGE} AS target

FROM ubuntu:24.04 AS scan
RUN apt-get update \
    && apt-get install -y --no-install-recommends openscap-scanner openscap-utils unzip \
    && rm -rf /var/lib/apt/lists/*
# ComplianceAsCode 0.1.82 carries DISA's RHEL 9 STIG V2R9.
ADD --checksum=sha256:765e84bdce7f9055f9b9c2dd0ee2b713d4255f8eec94eac6d35ea4973c28919c \
    https://github.com/ComplianceAsCode/content/releases/download/v0.1.82/scap-security-guide-0.1.82.zip /ssg.zip
RUN unzip -q -j /ssg.zip '*/ssg-rhel9-ds.xml' -d / && rm /ssg.zip
COPY --from=target / /target
# oscap exits 2 when a rule fails, which is a finding for the report, not a failed scan. Some rules govern the boot
# and the console, which a container has none of, so a report with failures is the expected result.
RUN oscap-chroot /target xccdf eval --profile stig --report /report.html \
      /ssg-rhel9-ds.xml > /results.txt; \
    s=$?; grep -c '^Result.*fail' /results.txt; test $s -eq 0 -o $s -eq 2

FROM scratch
COPY --from=scan /report.html /results.txt /
