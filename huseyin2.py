import xml.etree.ElementTree as ET

# Start to extract PSG sleep scores
mesaid = 1
profusion_file = "/home/huseyin/bitirme/mesa-benchmark/data/mesa/polysomnography/annotations-events-profusion/mesa-sleep-%04d-profusion.xml" % (mesaid)

tree = ET.parse(profusion_file)
root = tree.getroot()

stages = []

# for sleep_stage in root.findall(".//CMPStudyConfig/SleepStages/*"):
for sleep_stage in ["1","2","3","4","5"]:
    stages.append(sleep_stage.text)

print(stages)

