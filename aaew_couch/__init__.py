import re
import json
import logging
import couchdb

logging.basicConfig(level=logging.WARN)
log = logging.getLogger(__name__)

try:
    from tqdm import tqdm

    TQDM = True
except:
    TQDM = False

VIEW_WINDOW_SIZE = 512

REPL_DOC_TEMPL = """{{"source":{{"headers":{{}}, "url":{}}}, "target":{{"headers":{{}}, "url":{}}}, "filter":"{}", "continuous":false, "create_target":false,
}}"""


def connect(url, auth_file=None, user=None, passwd=None):
    """ connect to a couchdb server at the specified `URL`.

    Uses the contents of JSON file `auth_file` to login.  This JSON file is
    expected to contain a single object with the keys `user` and `pass`.
    Instead of the auth file, one can also just pass a username and a password
    with the parameters `user` and `passwd`.  """
    if auth_file:
        with open(auth_file, "r") as authfile:
            auth = json.load(authfile)
    elif user != None and passwd != None:
        auth = {"user": user, "pass": passwd}
    else:
        auth = None

    try:
        server = couchdb.Server(url)
        if auth:
            server.resource.credentials = (auth.get("user"), auth.get("pass"))
            server.login(auth.get("user"), auth.get("pass"))
        server.version()
        return server
    except Exception as e:
        raise ConnectionError(
            "could not login to {}: {}".format(server.resource.url, e)
        )


PUBLIC_REVISIONSTATES = [
    "published",
    "transformed_awaiting_update",
    "published-awaiting-review",
    "published-obsolete",
    "archived-published-obsolete",
]

""" can be used as a temporary view that retrieves all published documents from a collection. """
TEMP_VIEW_PUB_DOC_IDS = """function(doc) {{
    if (doc.state == 'active' && doc.visibility == 'public' && ({})) {{
        emit(doc.id);
    }}
}}""".format(
    " || ".join(
        ["doc.revisionState == '{}'".format(state) for state in PUBLIC_REVISIONSTATES]
    )
)

_temp_view_published_docs_template = """function(doc) {{
    if (doc.eClass.split('/').pop() == '{}') {{
        if (doc.state == 'active' && doc.visibility == 'public' && ({})) {{
            emit(doc.id{});
        }}
    }}
}}"""


def list_views(collection) -> list:
    """ finds a given collection's `_design`-docs and extracts the view names
    found inside of them.

    :param collection: couchdb collection
    :type collection: :class:`couchdb.Database`
    :rtype: list
    """
    desdocs = [collection[name] for name in collection if name.startswith("_design/")]
    views = []
    for doc in desdocs:
        path = doc.id.split("/")[-1]
        views.extend(map(lambda v: path + "/" + v, doc.get("views", {}).keys()))
    return views


def temp_view_published_docs(eclass, *fields):
    """ Returns a temporary view function string that can be used to retrieve
    all published documents with a specified ``eClass``-suffix from a
    collection (i.e. ``BTSLemmaEntry``).

    By default, only the ``doc.id`` field is being selected by this view
    function.  To receive a different selection of ``doc`` fields, those can be
    specified as string value parameters in arbitrary numbers. ``doc.id`` will
    be selected no matter what tho. The fields to be selected have to start
    with '`doc`', but that means that it is also possible to request the whole
    document.

    The returned view function string can be used with :ref:`apply_temp_view`.

    :param eclass: class part in EMF eClass URL string (last segment)
    :type eclass: string
    :rtype: string
    """
    return _temp_view_published_docs_template.format(
        eclass,
        " || ".join(
            map(
                lambda state: "doc.revisionState == '{}'".format(state),
                PUBLIC_REVISIONSTATES,
            )
        ),
        ", doc"
        if "doc" in fields
        else ", {{{}}}".format(
            ", ".join(
                map(lambda f: "'{}': {}".format(".".join(f.split(".")[1:]), f), fields)
            )
        )
        if len(fields) > 0
        else "",
    )


def apply_view(collection, view_name: str):
    """ applies collection-internal view to collection and returns generator.

    :param collection: couchdb collection
    :type collection: :class:`couchdb.Database`
    :param view_name: name of a view that must exist in the collection
    :rtype: generator
    """
    skip = 0
    results_pending = True
    window_size = VIEW_WINDOW_SIZE
    log.debug('applying saved view {} with page size {}'.format(
        view_name,
        window_size))
    while results_pending:
        """ query database collection with stored view for a limited number
            of results """
        view_result = collection.view(view_name, skip=skip, limit=window_size)
        """ see if we need to query the view another time:
            i.e. if we have less results than set limit """
            #TODO handle MemoryError
        results_pending = skip + len(view_result.rows) < view_result.total_rows
        """ move window for upcoming query invocation """
        skip += window_size
        """ produce results """
        for row in view_result.rows:
            if row.value:
                value = row.value
                value["id"] = row.id
                yield row.value
            else:
                yield row.id


#XXX problem: only possible as admin
def view_result_count(collection, view: str) -> int:
    """ Returns the number of rows that would be returned when a given view
    is queried on that database collection.

    Both view names and view functions can be passed. The function determines
    what the parameter value actually is by looking it up in the collection's
    view list.

    :param collection: couchdb collection
    :type collection: :class:`couchdb.Database`
    :param view: view name or function
    :returns: result count or -1
    """
    func = collection.view if view in list_views(collection) else collection.query
    try:
        view_result = func(view, limit=0)
        return view_result.total_rows
    except:
        return -1


def apply_temp_view(collection, view_function):
    """ takes a view function as a string and applies it to the collection.
    Is a generator that returns either the object emitted by the view function,
    or the id of each documents in the results (whatever applicable).

    :param collection: the collection to be queried
    :type collection: :class:`couchdb.Database`
    :returns: an interator over the view results
    :rtype: generator
    """
    skip = 0
    results_pending = True
    window_size = VIEW_WINDOW_SIZE
    log.debug('applying temporary view with page size {}'.format(window_size))
    while results_pending:
        """ query database collection with temporary view for limited number
            of results """
        try:
            view_result = collection.query(
                view_function, skip=skip, limit=window_size
            )
            """ see if we need to query the view another time:
                i.e. if we have less results than set limit """
            results_pending = skip + len(view_result.rows) < view_result.total_rows
            if window_size < VIEW_WINDOW_SIZE:
                window_size += window_size // 10
        except couchdb.http.ServerError as e:
            raise ValueError("server cannot execute view")
            return
        except MemoryError as e:
            if window_size > 4:
                """ make window smaller and try again """
                window_size //= 4
                log.warn('adjusting window size: {}'.format(window_size))
                continue
            raise(e)

        """ move window for upcoming query invocation """
        skip += window_size
        """ produce results """
        for row in view_result.rows:
            if row.value:
                value = row.value
                value["id"] = row.id
                yield row.value
            else:
                yield row.id


def is_document_public(document: dict) -> bool:
    """ checks whether document `visibility` and `revisionState` match
    requirements to be considered ready for publication.

    :param document: couchdb document
    :returns: whether visibility and revision state qualify for publication
    """
    if document.get("visibility") == "public":
        return document.get("revisionState") in PUBLIC_REVISIONSTATES
    return False


def retrieve_public_documents(collection):
    """ Generates a collection's list of documents filtered by the ad-hoc view
    `TEMP_VIEW_PUB_DOC_IDS` that takes into account each document's `state`,
    `visibility`, and `revisionState`.

    Returns a generator that downloads each document one by one, which is
    preposterously slow, but makes sure that huge corpora won't cause heap
    overflows.

    :paran collection: couchdb collection
    :type collection: :class:`couchdb.Database`
    :rtype: generator
    """
    view = collection.query(TEMP_VIEW_PUB_DOC_IDS)
    pb = tqdm(total=view.total_rows, desc=collection.name) if TQDM else None
    for row in view:
        if row.id:
            yield collection[row.id]
            if pb:
                pb.update(1)


def public_corpora_of_project(server, prefix):
    """ Extract public `BTSTextCorpus` objects with revision state 'published'
    or 'transformed_awaiting_update'. """
    if "{}_corpus".format(prefix) in server:
        corpora_view = server["{}_corpus".format(prefix)].view(
            "corpus/all_active_btstextcorpus"
        )
        for row in corpora_view:
            corpus = row.value
            if is_document_public(corpus):
                collection_name = "{}_corpus_{}".format(
                    prefix,
                    corpus.get("corpusPrefix")
                )
                if collection_name in server:
                    try:
                        yield server[collection_name]
                    except:
                        pass


def get_projects(server):
    """ returns list of project prefixes """
    return [row.value for row in server["admin"].view("admin/all_active_projects")]


def all_public_collections(server):
    """ Retrieves public corpora of all projects found in the
    `admin/all_projects` view on the `admin` collection on couchdb instance
    `aaew64`, by using the `public_corpora_of_project` function.  Returns a
    tuple of which the first element is a list of collection names, and the
    seconds element is a list of word list and thesaurus collections. """
    corp = {"corpus": [], "wlist": [], "ths": [], "admin": []}

    # go through results from projects view in admin collection and extract each project's
    # corpora names from its `dbCollections` record
    for row in server["admin"].view("admin/all_active_projects"):
        project = row.value
        collections = [
            c.get("collectionName") for c in project.get("dbCollections", [])
        ]

        # rely on each project to specify a prefix string for its collections
        if project.get("prefix"):
            prefix = project.get("prefix")

            # add vocabularies (word list and thesaurus collection) if existing and configured
            for suffix in ["wlist", "ths", "admin"]:
                collection_name = "{}_{}".format(prefix, suffix)
                if collection_name.format(prefix) in server:
                    if collection_name.format(prefix) in collections:
                        try:
                            corp[suffix].append(server[collection_name])
                        except:
                            pass

            # extract `published` (or `transformed_awaiting_update`) corpora from the {prefix}_corpus collection
            # associated with the project
            corp["corpus"].extend(
                [
                    c
                    for c in public_corpora_of_project(server, prefix)
                    if c.name in collections
                ]
            )
    return corp


def all_active_btsusers(server, usergroups=True):
    """ returns a generator that produces all `BTSUser` documents from the
    server's `admin` collection, and, by default, all `BTSUserGroups` as well,
    unless the parameter `usergroups` is set to `False`. """
    for d in apply_view(server["admin"], "admin/all_active_btsusers"):
        yield d
    if usergroups:
        for d in apply_view(server["admin"], "admin/all_active_btsusergroups"):
            yield d


__all__ = [
    "PUBLIC_REVISIONSTATES",
    "TEMP_VIEW_PUB_DOC_IDS",
    "temp_view_published_docs",
    "connect",
    "list_views",
    "get_projects" "apply_view",
    "apply_temp_view",
    "view_result_count",
    "is_document_public",
    "retrieve_public_documents",
    "all_public_corpora",
    "public_corpora_of_project",
    "all_active_btsusers",
]
