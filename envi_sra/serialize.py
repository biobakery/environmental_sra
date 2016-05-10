from os.path import basename
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import SubElement as sub

from . import geo


def prep_subtype(p):
    return p._get_raw_doc()['meta']['subtype']

def eld(tagname, attrs={}, text=None, children=[]):
    ret = {"tagname": tagname}
    if attrs:
        ret['attrs'] = attrs
    if text:
        ret['text'] = text
    if children:
        ret['children'] = children
    return ret

def hier_sub(parent, tagname, attrs={}, text=None, children=[]):
    el = sub(parent, tagname, attrs)
    if text:
        el.text = text
    ret = [el]
    for kwargs in children:
        ret.append(hier_sub(el, **kwargs))
    return ret


def very_last(nested_list):
    last = nested_list[-1]
    while hasattr(last, "__iter__"):
        last = last[-1]
    return last


def spuid(obj):
    return eld("SPUID", attrs={"spuid_namespace": "hmp2"}, text=obj.id)


def flatten_list(l):
    def _flat(ls):
        for item in ls:
            if hasattr(item, "__iter__"):
                for subitem in _flat(item):
                    yield subitem
            else:
                yield item
    return list(_flat(l))


# Thanks, http://stackoverflow.com/a/4590052
def indent(elem, level=0):
    i = "\n" + level*"  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for elem in elem:
            indent(elem, level+1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

def reg_text(t):
    return " ".join(t.split())

def reg_sample(s):
    s['lat_lon'] = " ".join(geo.cardinal(s['lat_lon']))
    return s


def _add_description(root, st):
    hier_sub(root, "Description", children=[
        eld("Comment", text=st.name),
        eld("Organization", attrs={"role":"owner", "type":"institute"},
            children=[
                eld("Name", text="HSPH Department of Biostatistics"),
                eld("Contact", attrs={"email":"schwager@hsph.harvard.edu"},
                    children=[ eld("Name", children=[
                        eld("First", text="Randall"),
                        eld("Last",  text="Schwager")
                    ])
                ])
            ]
        )
    ])
    return root


def _add_bioproject(root, st):
    ret = hier_sub(root, "Action", children=[
        eld("AddData", attrs={"target_db":"BioProject"}, children=[
            eld("Data", attrs={"content_type":"xml"}, children=[
                eld("XmlContent", children=[
                    eld("Project", attrs={"schema_version":"2.0"})
                ])
            ]),
            eld("Identifier", children=[spuid(st)]),
        ])
    ])
    prj = flatten_list(ret)[-3]
    hier_sub(prj, "ProjectID", children=[spuid(st)])
    hier_sub(prj, "Descriptor", children=[
        eld("Title",    text=st.name),
        eld("Description", text=reg_text(st.description)),
        eld("Relevance", children=[ eld("Medical", text="Yes") ])
    ])
    pts_attrs = {"sample_scope":"eEnvironment"}
    hier_sub(prj, "ProjectType", children=[
        eld("ProjectTypeSubmission", attrs=pts_attrs, children=[
            eld("IntendedDataTypeSet", children=[
                eld("DataType", text="metagenome")
            ])
        ])
    ])
    return root


reqd_mims_keys = ['rel_air_humidity', 'organism_count',
                  'abs_air_humidity', 'lat_lon', 'env_feature',
                  'heat_cool_type', 'collection_date',
                  'space_typ_state', 'ventilation_type', 'env_biome',
                  'geo_loc_name', 'building_setting',
                  'typ_occupant_dens', 'indoor_space', 'filter_type',
                  'env_material', 'occup_samp', 'build_occup_type',
                  'air_temp', 'carb_dioxide', 'occupant_dens_samp',
                  'light_type']


def _add_biosample(root, st, sample):
    sample = reg_sample(sample)
    ret = hier_sub(root, "Action", children=[
        eld("AddData", attrs={"target_db":"BioSample"}, children=[
            eld("Data", attrs={"content_type":"xml"}, children=[
                eld("XmlContent", children=[
                    eld("BioSample", attrs={"schema_version":"2.0"})
                ])
            ]),
            eld("Identifier", children=[spuid(sample)])
        ])
    ])
    bs_node = flatten_list(ret)[-3]
    hier_sub(bs_node, "SampleId", children=[spuid(sample)])
    hier_sub(bs_node, "Descriptor", children=[
        eld("Title", text="College campus dust sample"),
    ])
    hier_sub(bs_node, "Organism",
             attrs={"taxonomy_id": "256318"},
             children=[eld("OrganismName", text="Metagenome")])
    hier_sub(bs_node, "Package", text="MIMS.me.built.4.0")
    kv = lambda k, v: eld("Attribute", attrs={"attribute_name": k}, text=v)
    get = lambda v: sample.get(v, "missing")
    hier_sub(bs_node, "Attributes", children=[
        kv(name, get(name)) for name in reqd_mims_keys
    ])
    return root


def _add_sra(root, st, sample, seq):
    kv = lambda k, v: eld("Attribute", attrs={"name": k}, text=v)
    hier_sub(root, "Action", children=[
        eld("AddFiles", attrs={"target_db": "SRA"}, children=[
            eld("File", attrs={"file_path":basename(seq.path)},
                children=[eld("DataType", text="generic-data")]),
            kv("instrument_model",seq.seq_model),
            kv("library_strategy",seq.lib_const),
            kv("library_source", "GENOMIC"),
            kv("library_selection", seq.lib_const),
            kv("library_layout", "FRAGMENT"),
            kv("library_construction_protocol", reg_text(seq.method)),
            eld("AttributeRefId", attrs={"name": "BioProject"}, children=[
                eld("RefId", children=[spuid(st)])
            ]),
            eld("AttributeRefId", attrs={"name": "BioSample"}, children=[
                eld("RefId", children=[spuid(sample)])
            ]),
            eld("Identifier", children=[spuid(seq)])
        ])
    ])
    return root


def to_xml(st, samples_seqs):
    root = ET.Element('Submission')
    root = _add_description(root, st)
    root = _add_bioproject(root, st)
    for sample, seq in samples_seqs:
        root = _add_biosample(root, st, sample)
        root = _add_sra(root, st, sample, seq)
    return root

