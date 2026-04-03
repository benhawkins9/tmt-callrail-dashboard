"""
Shared configuration: date range, client whitelist, tag definitions.
"""

from datetime import date

DATE_FROM = "2024-06-01"
DATE_TO   = date.today().isoformat()

# ---------------------------------------------------------------------------
# Client whitelist
# Only companies whose names fuzzy-match one of these entries will be synced
# and shown in the dashboard.  The matching strips punctuation and is
# case-insensitive so small API-side naming differences are handled.
# ---------------------------------------------------------------------------
CLIENT_WHITELIST = [
    "AGC Agency Inc",
    "Centra Networks",
    "Windstar Technologies Inc",
    "Tomorrow's Technology Today",
    "The Nerd Stuff",
    "Take Ctrl, LLC",
    "Superior Technical Solutions (IT4eyes)",
    "Superior Technical Solutions",
    "Simplified IT Solutions",
    "Shift Computer Services",
    "RTS, IT Inc",
    "Panther Computers",
    "OnPoint Technology Group, Inc",
    "Network Providers Inc",
    "Nerds In A Flash",
    "Natural Networks, Inc",
    "Merit Technology Solutions",
    "Maise Technology",
    "LastTech",
    "Integrita Systems",
    "Integral Networks, Inc",
    "Integrated Business Systems",
    "GDS Technology",
    "Creative IT",
    "Converging Networks Group, Inc",
    "Computerware, Inc",
    "Braintek, LLC",
    "BeckTek",
    "Automates",
    "Argentum IT",
    "American Frontier, LLC",
    "Advent Technologies",
    "911 IT",
    "Cloudminders",
    "The Network Doctor, Inc",
    "ET&T",
    "TeamLogic IT of Boulder, CO",
    "TeamLogic IT",
    "CHR Creative",
    "Teamspring, Inc",
    "Systems Support",
    "Quantum Technologies",
    "Professional IT Solutions",
    "Navisus Technologies",
    "Johnson Business Technology Solutions, Inc",
    "JNT Tek IT Solutions",
    "J&B Technologies",
    "ISM Grid Corp",
    "Framework IT",
    "CNS Data Inc",
    "ArgoCTS",
    "Vitalpoints",
    "TS Conrad",
    "BroCoTec",
    "Vieth Consulting",
    "Veracity Technology",
    "Tri-State Computer Solutions",
    "TechSage Solutions",
    "T4 Group",
    "Southwest Networks",
    "Solve iT",
    "PC Solutions.Net",
    "Orbis Solutions Inc",          # "Cancelled - Orbis Solutions Inc"
    "NSN Management",
    "King Office Service, Inc",
    "KAMIND IT, Inc",
    "Eher Systems, LLC",
    "Blue Tree Technology",
    "CyberTrust IT Solutions",
    "BridgePoint Technologies, LLC",
    "BrevinIT Technologies",
    "BEL Network Integration & Support",
    "All-Access Infotech LLC",
    "Microtech",
    "Next Century Tech",
    "Tech Rage IT",
    "Creative Resources Technology",  # "Cancelled - Creative Resources Technology"
    "Continuous Networks, LLC",
    "ComTech Network Solutions",
    "Affiliated Resource Group",
    "IV Experts, Inc",
    "Sitrwan Systems",               # "Cancelled - Sitrwan Systems"
    "One82",                         # "Cancelled - One82"
    "Digital DataComm",
    "DenaliTEK Incorporated",        # "Cancelled - DenaliTEK Incorporated"
    "Corptek Solutions",
    "Active IT Solutions",           # "Cancelled - Active IT Solutions"
    "ACS Computer Services, Inc",
    "Your IT Guys",
    "The Ritte Group",
    "Sewelltech, Inc",
    "My PC Partners",
    "KME Technology Consultants LLC",
    "ITS Team",
    "Essential IT Services, Inc",
    "IT4eyes",
]

# ---------------------------------------------------------------------------
# Normalise a company name for fuzzy matching:
# lowercase, strip "cancelled - " prefix, remove punctuation
# ---------------------------------------------------------------------------
import re

def _norm(name: str) -> str:
    n = name.lower().strip()
    n = re.sub(r"^cancelled\s*[-–]\s*", "", n)
    n = re.sub(r"[^a-z0-9 ]", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n

_WHITELIST_NORMED = {_norm(c) for c in CLIENT_WHITELIST}


def is_whitelisted(company_name: str) -> bool:
    """
    Return True if the company name (from the API) matches any whitelist entry.
    Matching is case-insensitive, strips 'Cancelled - ' prefix, and ignores
    punctuation so minor naming differences don't cause misses.
    """
    normed = _norm(company_name)
    if normed in _WHITELIST_NORMED:
        return True
    # Also accept if any whitelist token is a full substring match
    for wl in _WHITELIST_NORMED:
        if wl in normed or normed in wl:
            return True
    return False
