"""The leader's REST API, called the way the GUI calls it: a session cookie, the Origin, and the CSRF token."""

import json
import os
import ssl
import urllib.error
import urllib.parse
import urllib.request

BASE = "https://127.0.0.1:8443"
SESSIONS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state", "sessions.json")

# The leader signs its own certificate, and a proxy in the environment has no business with a loopback call.
_ctx = ssl.create_default_context()
_ctx.check_hostname = False
_ctx.verify_mode = ssl.CERT_NONE
_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=_ctx))


class ApiError(Exception):
    pass


class Client:
    def __init__(self):
        self.cookie = None
        self.csrf = None

    @classmethod
    def signed_in(cls, username, password):
        """A client on the session this run already holds for `username`, or a new one.

        Reusing it matters: the leader throttles sign-ins, and a run signs in as three people many times over."""
        store = {}
        if os.path.exists(SESSIONS):
            with open(SESSIONS) as f:
                store = json.load(f)
        c = cls()
        if username in store:
            c.cookie, c.csrf = store[username]
            try:
                if c.call("GET", "/api/auth/session").get("authenticated"):
                    return c
            except ApiError:
                pass
            c.cookie = c.csrf = None
        c.login(username, password)
        store[username] = [c.cookie, c.csrf]
        os.makedirs(os.path.dirname(SESSIONS), exist_ok=True)
        with open(SESSIONS, "w") as f:
            json.dump(store, f)
        return c

    def call(self, method, path, body=None, params=None, content_type="application/json", data=None):
        url = BASE + path
        if params:
            url += ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
        headers = {"Origin": BASE, "Accept": "application/json"}
        if self.cookie:
            headers["Cookie"] = self.cookie
        if method not in ("GET", "HEAD") and self.csrf:
            headers["x-smeltr-csrf"] = self.csrf
        if data is None and body is not None:
            data = json.dumps(body).encode()
        if data is not None:
            headers["Content-Type"] = content_type
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with _opener.open(req) as res:
                text = res.read().decode()
                cookie = res.headers.get("Set-Cookie")
        except urllib.error.HTTPError as e:
            raise ApiError(f"{method} {path} -> HTTP {e.code}: {e.read().decode()[:500]}") from None
        if cookie:
            self.cookie = cookie.split(";", 1)[0]
        return json.loads(text)

    def login(self, username, password):
        s = self.call("POST", "/api/auth/login", {"username": username, "password": password})
        self.csrf = s.get("csrf")
        return s

    def change_password(self, username, new):
        return self.call("PUT", "/api/auth/password", {"username": username, "newPassword": new})

    # -- config, on a branch and in an area -------------------------------------------------------------

    def cfg(self, method, path, branch, scope, body=None):
        """`path` within area `scope`, on `branch`."""
        res = self.call(method, f"/api/areas/{scope}{path}", body, {"branch": branch})
        if isinstance(res, dict) and res.get("ok") is False:
            errors = json.dumps(res.get("errors") or res.get("error"))[:1500]
            raise ApiError(f"{method} {path} [{branch}/{scope}] refused: {errors}")
        return res

    def put(self, what, items, branch, scope):
        return self.cfg("PUT", f"/{what}", branch, scope, items)

    def put_pipeline(self, p, branch, scope):
        return self.cfg("PUT", f"/pipelines/{urllib.parse.quote(p['id'])}", branch, scope, p)

    def put_sample(self, s, branch, scope):
        return self.cfg("PUT", f"/samples/{urllib.parse.quote(s['id'])}", branch, scope, s)

    def preview(self, branch, scope, pipeline, sample, at_micros):
        # The config the editor sends (`buildFileConfig` in pipeline.ts): this scope's pipelines and tables, and
        # nothing routed, so a route naming another scope's destination cannot fail the preview.
        full = self.cfg("GET", "", branch, scope)["config"]
        config = {"version": 1, "destinations": [], "lookupTables": full["lookupTables"],
                  "pipelines": full["pipelines"]}
        body = {"config": json.dumps(config), "pipeline": pipeline, "sample": sample,
                "show_internal": False, "at_micros": at_micros}
        return self.call("POST", f"/api/areas/{scope}/preview/pipeline", body, {"branch": branch})

    def lock_test(self, branch, scope, name, pipeline, sample, records, at_micros):
        pv = self.preview(branch, scope, pipeline, sample, at_micros)
        if not pv["ok"]:
            raise ApiError(f"preview of {pipeline} failed: {pv['errors']}")
        test = {"id": name, "name": name, "pipeline": pipeline, "sample": sample, "lockedAt": at_micros,
                "rowsIn": records, "expected": [r["json"] for r in pv["rows"]]}
        return self.cfg("PUT", f"/tests/{urllib.parse.quote(name)}", branch, scope, test)

    # -- changes ------------------------------------------------------------------------------------

    def open_change(self, branch, title, description=""):
        return self.ok(self.call("POST", f"/api/branches/{urllib.parse.quote(branch)}/open", {"title": title, "description": description}))

    def deploy(self, branch):
        return self.ok(self.call("POST", f"/api/branches/{urllib.parse.quote(branch)}/deploy", {}))

    def create_group(self, branch, gid, name, hue):
        return self.ok(self.call("POST", "/api/worker-groups", {"id": gid, "name": name, "hue": hue}, {"branch": branch}))

    @staticmethod
    def ok(res):
        if isinstance(res, dict) and res.get("ok") is False:
            raise ApiError(json.dumps(res)[:1500])
        return res
