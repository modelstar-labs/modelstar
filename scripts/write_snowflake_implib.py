import json

file_path = './anaconda_channeldata.json'

with open(file_path) as f:
    data = json.load(f)

snow_pkg = {}
for pk, value in data['packages'].items():
    snow_pkg[pk] = {'version': value['version']}

with open("snowflake_implib.py", 'w') as f:
    f.write('anaconda = {\n')
    for key, value in snow_pkg.items():
        f.write("\t'%s': %s,\n" % (key, value))
    f.write('}\n')
