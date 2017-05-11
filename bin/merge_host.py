import sys

if len(sys.argv) != 5:
  print ("Usage: python bin/merge_stat_res.py \
    [parsed_res.txt] [selected_res.txt] [fetched_res.txt] [in_norm_res.txt]")
  exit(1)

parsed_res   = sys.argv[1]
selected_res = sys.argv[2]
fetched_res  = sys.argv[3]
in_norm_res  = sys.argv[4]

def read_list(fname):
  try:
    l = []
    with open(fname) as fi:
      for line in fi:
        parts = line.split(",") 
        if len(parts) != 2: continue
        host  = parts[0].strip()[2:-1]
        count = parts[1].strip()[:-1]
        l.append((host, int(count)))
    return l
  except:
    return []

l1 = read_list(parsed_res)
l2 = read_list(selected_res)
l3 = read_list(fetched_res)
l4 = read_list(in_norm_res)

if len(l1)==0 and len(l2)==0 and len(l3)==0 and len(l4) == 0:
  exit(0)

m = {}
if len(l1) > 0:
  for e in l1:
    m[e[0]] = [e[1], 0, 0, 0]

if len(l2) > 0:
  for e in l2:
    if e[0] not in m:
      m[e[0]] = [0, e[1], 0, 0]
    else:
      m[e[0]][1] = e[1]

if len(l3) > 0:
  for e in l3:
    if e[0] not in m:
      m[e[0]] = [0, 0, e[1], 0]
    else:
      m[e[0]][2] = e[1]

if len(l4) > 0:
  for e in l4:
    if e[0] in m:
      m[e[0]][3] = e[1]

for k, v in m.items():
  print ("%s\t%d\t%d\t%d\t%d" % (k, v[0], v[1], v[2], v[3]) )
