"""
Topology view for the dashboard diagram.

Builds the list of backend nodes the network diagram draws, from three real sources:

  registered servers   the reverse proxy's server registry (the routing pool is a subset)
  candidates           replacement processes being provisioned right now, by stage-1 automatic
                       recovery or by stage-2 capacity scale-out; never routable until registered
  retired              replacements the capacity manager stopped recently (scale-in or failure);
                       shown greyed out without a link for a few seconds, never as active

No node is invented: the number of nodes always equals registered + in-progress + recently
retired. A desired count larger than the registered count is reported as a number only.
"""

from typing import Any, Dict, List, Optional

# auto-recovery episode stages in which a candidate process is being provisioned
RECOVERY_ACTIVE_STAGES = ("provisioning", "started", "health_check", "healthy", "failed")
RECOVERY_STAGE_LABEL = {"provisioning": "PROVISIONING", "started": "STARTING", "health_check": "HEALTH CHECK",
                        "healthy": "REGISTERING", "failed": "FAILED"}
CAPACITY_STAGE_LABEL = {"allocating": "PROVISIONING", "starting": "STARTING", "health_check": "HEALTH CHECK",
                        "reconciling": "REGISTERING"}


def _server_node(s: Dict[str, Any]) -> Dict[str, Any]:
    if s.get("draining"):
        state = "DRAINING"
    elif s.get("standby") and s["status"] == "HEALTHY":
        state = "STANDBY"
    else:
        state = s["status"]
    routable = bool(s.get("routable"))
    if routable:
        why = "in routing pool"
    elif s.get("draining"):
        why = "draining: no new requests"
    elif s.get("standby"):
        why = "standby: running and health-checked, not routed until load rises"
    elif s["status"] == "UNHEALTHY":
        why = "failed: removed from routing"
    elif s["status"] == "RECOVERING":
        why = "probation: not routed until verified"
    elif (s.get("circuit") or {}).get("state") == "OPEN":
        why = "circuit open: skipped"
    else:
        why = "not routable"
    return {"id": s["id"], "name": s["name"], "kind": "server", "role": s.get("origin") or "original",
            "state": state, "routable": routable, "eligibility": why, "port": s.get("port"), "pid": s.get("pid"),
            "requests_served": s.get("requests_served", 0), "active_connections": s.get("active_connections", 0)}


def build_topology(servers: List[Dict[str, Any]], recovery: Optional[Dict[str, Any]],
                   capacity: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    registered = {s["id"] for s in servers}
    nodes = [_server_node(s) for s in servers]

    ep = (recovery or {}).get("episode") or None
    if ep and ep.get("candidate") and ep.get("stage") in RECOVERY_ACTIVE_STAGES:
        c = ep["candidate"]
        healthy_exists = any(s["status"] == "HEALTHY" for s in servers)
        if c.get("id") and c["id"] not in registered and not (ep["stage"] == "failed" and healthy_exists):
            nodes.append({"id": c["id"], "name": c.get("name") or c["id"], "kind": "candidate", "role": "recovery",
                          "state": RECOVERY_STAGE_LABEL[ep["stage"]], "stage": ep["stage"], "routable": False,
                          "eligibility": "provisioning failed: not registered" if ep["stage"] == "failed"
                          else "not registered yet: no traffic", "port": c.get("port"), "pid": c.get("pid"),
                          "error": ep.get("error")})

    prov = (capacity or {}).get("provisioning") or None
    if prov and prov.get("id") and prov["id"] not in registered:
        nodes.append({"id": prov["id"], "name": prov.get("name") or prov["id"], "kind": "candidate", "role": "capacity",
                      "state": CAPACITY_STAGE_LABEL.get(prov.get("stage"), str(prov.get("stage", "")).upper()),
                      "stage": prov.get("stage"), "routable": False, "eligibility": "not registered yet: no traffic",
                      "port": prov.get("port"), "pid": prov.get("pid")})

    shown = {n["id"] for n in nodes}
    for r in reversed((capacity or {}).get("retired") or []):
        if r["id"] in shown:
            continue
        shown.add(r["id"])
        nodes.append({"id": r["id"], "name": r["name"], "kind": "retired", "role": r.get("origin") or "capacity",
                      "state": "RETIRED", "routable": False, "eligibility": f"retired: {r.get('reason', '')}",
                      "port": r.get("port"), "pid": None, "age_s": r.get("age_s")})

    routable = sum(1 for n in nodes if n["routable"])
    healthy = sum(1 for s in servers if s["status"] == "HEALTHY" and not s.get("draining"))
    pending = sum(1 for n in nodes if n["kind"] == "candidate" and n["state"] != "FAILED")
    return {
        "nodes": nodes,
        "registered": len(servers),
        "routable": routable,
        "healthy": healthy,
        "pending_provisions": pending,
        "replacements_active": sum(1 for s in servers if s.get("is_replacement") and not s.get("draining")),
        "draining": sum(1 for s in servers if s.get("draining")),
        "standby": sum(1 for s in servers if s.get("standby")),
        "desired": (capacity or {}).get("desired"),
        "note": "nodes = registered servers + replacements being provisioned + recently retired replacements",
    }
