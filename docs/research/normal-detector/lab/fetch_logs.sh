#!/bin/sh
cd "${1:-../fetch}" && mkdir -p loghub ita
get() { [ -s "$2" ] || curl -sSL --retry 3 --max-time 1800 -o "$2" "$1"; }
get "https://zenodo.org/records/8196385/files/SSH.tar.gz?download=1" loghub/SSH.tar.gz
get "https://zenodo.org/records/8196385/files/Zookeeper.tar.gz?download=1" loghub/Zookeeper.tar.gz
get "https://zenodo.org/records/8196385/files/Apache.tar.gz?download=1" loghub/Apache.tar.gz
get "https://zenodo.org/records/8196385/files/Linux.tar.gz?download=1" loghub/Linux.tar.gz
get "https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz" ita/NASA_access_log_Jul95.gz
get "https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz" ita/NASA_access_log_Aug95.gz
get "https://ita.ee.lbl.gov/traces/calgary_access_log.gz" ita/calgary_access_log.gz
get "https://ita.ee.lbl.gov/traces/clarknet_access_log_Aug28.gz" ita/clarknet_access_log_Aug28.gz
get "https://ita.ee.lbl.gov/traces/clarknet_access_log_Sep4.gz" ita/clarknet_access_log_Sep4.gz
get "https://ita.ee.lbl.gov/traces/usask_access_log.gz" ita/usask_access_log.gz
get "https://zenodo.org/records/8196385/files/BGL.zip?download=1" loghub/BGL.zip
for t in SSH Zookeeper Apache Linux; do
    [ -d "loghub/$t" ] || { mkdir -p "loghub/$t" && tar xzf "loghub/$t.tar.gz" -C "loghub/$t"; }
done
[ -d loghub/BGL ] || { mkdir -p loghub/BGL && unzip -q loghub/BGL.zip -d loghub/BGL; }
echo done > dl.done
