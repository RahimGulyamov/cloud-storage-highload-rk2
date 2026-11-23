#!/usr/bin/env python3
"""Verify local report integrity and the independently useful capacity model."""
import hashlib
import json
import re
import struct
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    subprocess.run([sys.executable, str(ROOT/'model/calculate.py'), '--check'], check=True)
    doc = (ROOT/'README.md').read_text()
    assert doc.count('```') % 2 == 0, 'Unclosed fenced code block'
    assert not re.search(r'\b(?:TODO|TBD|FIXME|PLACEHOLDER)\b', doc), 'Unfinished placeholder'
    anchors = set(re.findall(r'<a id="([^"]+)"></a>', doc))
    links = re.findall(r'!?\[[^\]]*\]\(([^)]+)\)', doc)
    checked = []
    for link in links:
        if link.startswith(('https://', 'http://')):
            continue
        if link.startswith('#'):
            assert link[1:] in anchors, f'Broken anchor: {link}'
        else:
            assert (ROOT/link).is_file(), f'Missing local reference: {link}'
        checked.append(link)
    tables = []
    for match in re.finditer(r'(?m)^\|.*\n(?:\|.*\n)+', doc):
        rows = match.group().strip().splitlines()
        counts = [len(re.findall(r'(?<!\\)\|', line)) for line in rows]
        assert len(set(counts)) == 1, f'Malformed table near {rows[0]}'
        tables.append(len(rows)-2)
    diagrams = []
    for stem in ['architecture','upload-sequence']:
        puml = (ROOT/f'docs/{stem}.puml').read_text()
        assert puml.startswith('@startuml') and puml.rstrip().endswith('@enduml')
        png = (ROOT/f'docs/{stem}.png').read_bytes()
        assert png[:8] == b'\x89PNG\r\n\x1a\n', 'Invalid PNG'
        width,height = struct.unpack('>II',png[16:24])
        assert width > 500 and height > 400, 'Diagram is unexpectedly small'
        svg = ET.fromstring((ROOT/f'docs/{stem}.svg').read_text())
        all_text = ' '.join(svg.itertext())
        assert 'Syntax Error' not in all_text and 'An error has' not in all_text, 'PlantUML error image'
        assert len(list(svg.iter())) > 50
        diagrams.append({'name':stem,'width':width,'height':height,
                         'png_sha256':hashlib.sha256(png).hexdigest()})
    source_refs = {int(n) for n in re.findall(r'(?m)^(\d+)\. \[',doc)}
    assert source_refs == set(range(1,23)), 'Unexpected source bibliography'
    result = {'status':'PASS','capacity_checks':14,'markdown_tables':len(tables),
              'local_links_and_anchors':len(checked),'sources':len(source_refs),'diagrams':diagrams}
    (ROOT/'model/validation.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__ == '__main__':
    main()
