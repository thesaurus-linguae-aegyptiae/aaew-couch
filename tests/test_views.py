import os
import pytest

SERVER_URL = os.environ["COUCHDB_SERVER_URL"]

def test_import():
    import aaew_couch
    assert aaew_couch is not None


def test_connect_with_auth_missing():
    from aaew_couch import connect
    with pytest.raises(ConnectionError):
        server = connect(SERVER_URL)


def test_connect_with_auth():
    from aaew_couch import connect
    server = connect(SERVER_URL,
            auth_file='auth.json')
    assert server is not None


def test_connect_with_auth_file_missing():
    from aaew_couch import connect
    with pytest.raises(FileNotFoundError):
        server = connect(SERVER_URL, auth_file='nonexistent')


def test_temp_view_filter_eclass_only_id():
    from aaew_couch import connect, temp_view_published_docs, apply_temp_view
    server = connect(SERVER_URL,
            auth_file='auth.json')
    view = temp_view_published_docs('BTSText')
    view_gen = apply_temp_view(server['aaew_corpus_bbawtestcorpus'], view)
    d = next(view_gen)
    assert d is not None
    assert type(d) is str


def test_temp_view_filter_eclass_spec_fields():
    from aaew_couch import connect, temp_view_published_docs, apply_temp_view
    server = connect(SERVER_URL,
            auth_file='auth.json')
    view = temp_view_published_docs('BTSText', 'doc.name', 'doc.type')
    view_gen = apply_temp_view(server['aaew_corpus_bbawtestcorpus'], view)
    d = next(view_gen)
    assert d is not None
    assert type(d) is dict
    assert d.get('id') is not None


def test_temp_view_syntax_error():
    from aaew_couch import connect, apply_temp_view
    server = connect(SERVER_URL,
            auth_file='auth.json')
    view_func = 'function(doc){'
    view = apply_temp_view(server['admin'], view_func)
    with pytest.raises(ValueError):
        next(view)


def test_project_view():
    from aaew_couch import connect, get_projects
    server = connect(SERVER_URL,
            auth_file='auth.json')
    projects = get_projects(server)
    assert projects is not None
    assert len(projects) > 0


def test_btsusers_views():
    from aaew_couch import connect, all_active_btsusers
    server = connect(SERVER_URL,
            auth_file='auth.json')
    usergen = all_active_btsusers(server)
    assert next(usergen) is not None
    assert next(usergen).get('eClass').endswith('BTSUser')
    assert any(map(lambda d:d.get('eClass').endswith('BTSUserGroup'),
        usergen))
    usergen = all_active_btsusers(server, usergroups=False)
    assert not any(map(lambda d:d.get('eClass').endswith('BTSUserGroup'),
        usergen))


def test_view_result_counts():
    from aaew_couch import (
        connect,
        view_result_count,
        temp_view_published_docs
    )
    server = connect(SERVER_URL,
            auth_file='auth.json')
    assert view_result_count(
            server['aaew_corpus_bbawtestcorpus'],
            'project_corpus/all_active_thsentry_objects') < 1
    assert view_result_count(
            server['aaew_ths'],
            'ths/all_active_thsentry_objects') > 1
    temp_view = temp_view_published_docs('BTSText',
            'doc.name', 'doc.type')
    #XXX must be admin
    assert view_result_count(
            server['aaew_corpus_bbawtestcorpus'],
            temp_view) > 0


def test_view_paged_window_size_adjustment():
    import aaew_couch as c
    server = c.connect(SERVER_URL,
            auth_file='auth.json')
    collection = 'aaew_wlist'
    c.VIEW_WINDOW_SIZE=50000
    temp_view = c.temp_view_published_docs('BTSLemmaEntry', 'doc')
    total = c.view_result_count(
        server[collection],
        temp_view
    )
    count = 0
    for doc in c.apply_temp_view(server[collection], temp_view):
        count += 1
    assert count == total



