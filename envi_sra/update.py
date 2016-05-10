import sys
import xml.etree.ElementTree as ET

def handle_error(r):
    obj_id = r.iter("Object").next().get("spuid")
    msg_text = "Problem with: " + obj_id + "\n"
    for m in r.iter():
        if m.text and m.text.strip():
            msg_text += m.text.strip() + "\n"
    return msg_text


def handle_ok(r):
    spuids = list()
    for obj in r.iter("Object"):
        spuids.append(obj.attrib['spuid'])
    return " ".join(spuids)


def print_report(report_fname):
    resps = ET.parse(report_fname).getroot().iter("Response")
    oks, errors = list(), list()
    for resp in resps:
        if 'status' not in resp.attrib:
            continue
        s = resp.attrib['status']
        if 'ok' in s or 'continue' in s:
            oks.append(handle_ok(resp))
        else:
            errors.append(handle_error(resp))

    for ok in oks:
        print >> sys.stderr, "OK --  "+ ok
        print >> sys.stderr, "-------"

    if errors:
        for error in errors:
            print >> sys.stderr, "ERROR --  "+error
            print >> sys.stderr, "----------"
        return False
