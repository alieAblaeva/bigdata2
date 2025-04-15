import sys
for row in sys.stdin:
    try:
        id_of_doc, title, context = row.strip().split('\t')
        print(f"{id_of_doc}\t{len(context.split())}")
    except Exception as e:
        pass
