# git clone https://github.com/AI4Bharat/IndicLID.git

mkdir -p models

wget https://github.com/AI4Bharat/IndicLID/releases/download/v1.0/indiclid-ftn.zip -P models/
wget https://github.com/AI4Bharat/IndicLID/releases/download/v1.0/indiclid-ftr.zip -P models/
wget https://github.com/AI4Bharat/IndicLID/releases/download/v1.0/indiclid-bert.zip -P models/

unzip models/indiclid-ftn.zip -d models/
unzip models/indiclid-ftr.zip -d models/
unzip models/indiclid-bert.zip -d models/

rm -rf models/*.zip