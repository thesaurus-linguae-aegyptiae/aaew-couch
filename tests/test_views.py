import pytest


def test_import():
    import aaew_couch
    assert aaew_couch is not None


def test_connect_with_auth_missing():
    from aaew_couch import connect
    with pytest.raises(ConnectionError):
        server = connect('http://aaew64.bbaw.de:9589')
    

def test_connect_with_auth():
    from aaew_couch import connect
    server = connect('http://aaew64.bbaw.de:9589',
            auth_file='auth.json')
    assert server is not None


def test_connect_with_auth_file_missing():
    from aaew_couch import connect
    with pytest.raises(FileNotFoundError):
        server = connect('http://aaew64.bbaw.de:9589', auth_file='nonexistent')


def test_temp_view_filter_eclass_only_id():
    from aaew_couch import connect, temp_view_published_docs, apply_temp_view
    server = connect('http://aaew64.bbaw.de:9589',
            auth_file='auth.json')
    view = temp_view_published_docs('BTSText')
    view_gen = apply_temp_view(server['aaew_corpus_bbawtestcorpus'], view)
    d = next(view_gen)
    assert d is not None
    assert type(d) is str


def test_temp_view_filter_eclass_spec_fields():
    from aaew_couch import connect, temp_view_published_docs, apply_temp_view
    server = connect('http://aaew64.bbaw.de:9589',
            auth_file='auth.json')
    view = temp_view_published_docs('BTSText', 'doc.name', 'doc.type')
    view_gen = apply_temp_view(server['aaew_corpus_bbawtestcorpus'], view)
    d = next(view_gen)
    assert d is not None
    assert type(d) is dict
    assert d.get('id') is not None


