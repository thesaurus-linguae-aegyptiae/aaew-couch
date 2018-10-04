import re
import json
import pprint
import couchdb
from tqdm import tqdm



rx_url = re.compile(r'^(https?://)(.*)$')

repl_doc_templ = '''{{"source":{{"headers":{{}}, "url":{}}}, "target":{{"headers":{{}}, "url":{}}}, "filter":"{}", "continuous":false, "create_target":false,
}}'''


def connect(url, auth_file=None, user=None, passwd=None):
    """ connect to a couchdb server at the specified `URL`. Use the contents of JSON file `auth_file` to login.
    This JSON file is expected to contain a single object with the keys `user` and `pass`. 
    Instead of the auth file, one can also just pass a username and a password with the parameters `user` and `passwd`.
    """
    if auth_file:
        with open(auth_file, 'r') as authfile:
            auth = json.load(authfile)
    elif user != None and passwd != None:
        auth = {'user': user, 'pass': passwd}
        
    try:
        server = couchdb.Server(url)
        server.resource.credentials = (auth.get('user'), auth.get('pass'))
        server.login(auth.get('user'), auth.get('pass'))
        return server
    except Exception as e:
        print('could not login to', server.resource.url)
        print(e)



public_revisionStates = ['published',
                         'transformed_awaiting_update',
                         'published-awaiting-update',
                         'published-obsolete']

publication_status_view = '''function(doc) {
    if (doc.state == 'active' && doc.visibility == 'public' && (doc.revisionState == 'published' || doc.revisionState == 'transformed_awaiting_update' || doc.revisionState == 'published-awaiting-update' || doc.revisionState == 'published-obsolete')) {
        emit(doc.id);
    }
}'''

published_document_view = lambda eclass: '''function(doc) {{
    if (doc.eClass.split('/').pop() == '{}') {{
        if (doc.state == 'active' && doc.visibility == 'public' && ({})) {{
            emit(doc.id, doc);
        }}
    }}
}}'''.format(eclass,
             ' || '.join(["doc.revisionState == '{}'".format(state)
                          for state in public_revisionStates]))


def list_views(collection):
    """ finds a given collection's `_design`-docs and extracts the view names found inside of them.
    """
    desdocs = [collection[name] for name in collection if name.startswith('_design/')]
    views = []
    for doc in desdocs:
        path = doc.id.split('/')[-1]
        views.extend(map(lambda v:path + '/' + v, doc.get('views', {}).keys()))
    return views



def apply_view(collection, view_name):
    """ applies collection-internal view to collection and returns generator. """
    for row in collection.view(view_name):
        yield row.value


def apply_temp_view(collection, view):
    """ takes a view function as a string and applies it to the collection.
    Is a generator. """
    for row in collection.query(view):
        yield row.value


def is_document_public(document):
    """ checks whether document `visibility` and `revisionState` match requirements to
    be considered ready for publication. """
    if document.get('visibility') == 'public':
        return document.get('revisionState') in public_revisionStates
    return False


def retrieve_public_documents(collection):
    """ Generates a collection's list of documents filtered by the ad-hoc view
    `publication_status_view` that takes into account each document's `state`,
    `visibility`, and `revisionState`.
    Returns a generator. """
    view = collection.query(publication_status_view)
    with tqdm(total=view.total_rows, desc=collection.name) as progressbar:
        for row in view:
            progressbar.update(1)
            if row.id:
                yield collection[row.id]


def public_corpora_of_project(server, prefix):
    """ Extract public `BTSTextCorpus` objects with revision state 'published'
    or 'transformed_awaiting_update'. """
    corpus_list = []
    if '{}_corpus'.format(prefix) in server:
        corpora_view = server['{}_corpus'.format(prefix)].view('corpus/all_active_btstextcorpus')
        for row in corpora_view:
            corpus = row.value
            if is_document_public(corpus):
                corpus_list.append('{}_corpus_{}'.format(prefix, corpus.get('corpusPrefix')))
    return [server[c] for c in corpus_list if c in server]


def all_public_corpora(server):
    """ Retrieves public corpora of all projects found in the `admin/all_projects`
    view on the `admin` collection on couchdb instance `aaew64`, by using the
    `public_corpora_of_project` function.
    Returns a tuple of which the first element is a list of collection
    names, and the seconds element is a list of word list and thesaurus collections. """
    corp = []
    for row in server['admin'].view('admin/all_projects'):
        project = row.value
        collections = [c.get('collectionName') for c in project.get('dbCollections', [])]
        if project.get('prefix'):
            prefix = project.get('prefix')
            #print(prefix, '-', project.get('name'))
            #pprint(collections)

            for suffix in ['wlist', 'ths']:
                collection_name = '{}_{}'.format(prefix, suffix)
                if collection_name.format(prefix) in server:
                    if collection_name.format(prefix) in collections:
                        corp.append(
                            (server[collection_name],
                             suffix))

            corp.extend([(c, 'corpus') for c in
                         public_corpora_of_project(server, prefix)])

    return corp


