import sys
import xml.etree.ElementTree as ET

def handle_error(r):
    obj_id = r.iter("Object").next().get("spuid")
    msg_text = "Problem with: " + obj_id + "\n"
    for m in r.iter():
        if m.text and m.text.strip():
            msg_text += m.text.strip() + "\n"
    return msg_text


def handle_ok(session, r):
    msg_text = ""
    osdf = session.get_osdf()
    for obj in r.iter("Object"):
        msg = ""
        try:
            doc = osdf.get_node(obj.attrib['spuid'])
            target_db = obj.attrib['target_db']
            acc_num = obj.attrib['accession']
        except Exception as e:
            msg += ("Unable to save object %s: %s"%(obj.attrib, e))
            msg_text += msg + "\n"
            continue
        to_append = "%s:%s"%(target_db, acc_num)
        if to_append not in doc['meta']['tags']:
            doc['meta']['tags'].append(to_append)
        _, errs = osdf.validate_node(doc)
        if errs:
            msg += "Unable to save changes to object `%s': %s"%(doc['id'], errs)
        else:
            osdf.edit_node(doc)
            msg = "%s id `%s' now has tags: %s"%(doc['node_type'],
                                                 doc['id'], doc['meta']['tags'])
        msg_text += msg+"\n"
    return msg_text


def update_osdf_from_report(session, report_fname):
    resps = ET.parse(report_fname).getroot().iter("Response")
    oks, errors = list(), list()
    for resp in resps:
        if 'status' not in resp.attrib:
            continue
        s = resp.attrib['status']
        if 'ok' in s or 'continue' in s:
            oks.append(handle_ok(session, resp))
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
