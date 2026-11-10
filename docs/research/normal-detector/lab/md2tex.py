"""paper.md -> paper.tex (python3 md2tex.py paper_v7.md). Inline math written as plain text in the markdown is set as LaTeX math."""
import re, sys

import sys
src = open(sys.argv[1] if len(sys.argv) > 1 else "paper_v5.md", encoding="utf-8").read()

# ---- explicit inline-math phrases, longest first (applied before the generic rules) --------------------------------
PHRASES = [
    ("P(X_{n+1} < X_{(r)}) = r/(n+1)", r"$P(X_{n+1} < X_{(r)}) = r/(n+1)$"),
    ("P(X_{n+1} < X_{(r)}) ≤ r/(n+1)", r"$P(X_{n+1} < X_{(r)}) \le r/(n+1)$"),
    ("X_{(1)} ≤ … ≤ X_{(n)}", r"$X_{(1)} \le \dots \le X_{(n)}$"),
    ("X_1, …, X_{n+1}", r"$X_1, \dots, X_{n+1}$"),
    ("X_1, …, X_n", r"$X_1, \dots, X_n$"),
    ("r ∈ {1, …, n}", r"$r \in \{1, \dots, n\}$"),
    ("s ∈ {0, …, D−1}", r"$s \in \{0, \dots, D-1\}$"),
    ("f ∈ {0, 0.1, 0.3, 0.5}", r"$f \in \{0, 0.1, 0.3, 0.5\}$"),
    ("w ∈ {1, 6}", r"$w \in \{1, 6\}$"),
    ("δ ∈ {0, 0.1, 0.2, 0.3, 0.4}", r"$\delta \in \{0, 0.1, 0.2, 0.3, 0.4\}$"),
    ("k ∈ {1, 2, 3, 4}", r"$k \in \{1, 2, 3, 4\}$"),
    ("{S_6(s) : s ∈ N(t)}", r"$\{S_6(s) : s \in N(t)\}$"),
    ("x_t ≥ 0, t ∈ ℤ", r"$x_t \ge 0$, $t \in \mathbb{Z}$"),
    ("L ⊂ ℤ", r"$L \subset \mathbb{Z}$"),
    ("Δ = 5 minutes; D = 288 buckets", r"$\Delta = 5$ minutes; $D = 288$ buckets"),
    ("(r+1)/(n+1)", r"$(r+1)/(n+1)$"),
    ("r/(n+1)", r"$r/(n+1)$"),
    ("1/(n+1)", r"$1/(n+1)$"),
    ("F + K·λ·M", r"$F + K\lambda M$"),
    ("λR/(λR + F)", r"$\lambda R/(\lambda R + F)$"),
    ("λR", r"$\lambda R$"),
    ("K·λ", r"$K\lambda$"),
    ("K × λ", r"$K \times \lambda$"),
    ("−log₁₀", r"$-\log_{10}$"),
    ("3.3 − κ", r"$3.3 - \kappa$"),
    ("(κ, H)", r"$(\kappa, H)$"),
    ("a bar H", r"a bar $H$"),
    ("(1−p)/2 and (1+p)/2", r"$(1-p)/2$ and $(1+p)/2$"),
    ("p = 99.9%", r"$p = 99.9\%$"),
    ("ℓ_1 / m", r"$\ell_1 / m$"),
    ("ℓ_6 / m", r"$\ell_6 / m$"),
    ("σ 0.6", r"$\sigma$ 0.6"),
    ("m(t) ± 99.9%", r"$m(t) \pm 99.9\%$"),
    ("m(t) × 99.9%", r"$m(t) \times 99.9\%$"),
    ("= 1 if", r"$= 1$ if"),
    ("and 0 otherwise", r"and $0$ otherwise"),
    ("S_w(t) < ℓ_w(t)", r"$S_w(t) < \ell_w(t)$"),
    ("S_6(t) > h_6(t)", r"$S_6(t) > h_6(t)$"),
    ("x_t^v", r"$x_t^v$"),
    ("field φ with value v", r"field $\varphi$ with value $v$"),
    ("with φ = v", r"with $\varphi = v$"),
    ("each value v of a watched field φ", r"each value $v$ of a watched field $\varphi$"),
    ("horizon τ", r"horizon $\tau$"),
    ("τ = 15", r"$\tau = 15$"),
    ("within τ", r"within $\tau$"),
    ("fraction f ", r"fraction $f$ "),
    ("factor f ", r"factor $f$ "),
    ("the effective n ", r"the effective $n$ "),
    ("whatever n is", r"whatever $n$ is"),
    ("wait k = 3", r"wait $k = 3$"),
    ("k ≥ 3", r"$k \ge 3$"),
    ("w−1 buckets", r"$w-1$ buckets"),
    ("width w ", r"width $w$ "),
    ("k−1 readings", r"$k-1$ readings"),
    ("reached k.", r"reached $k$."),
    ("of k readings", r"of $k$ readings"),
    ("k-th consecutive", r"$k$-th consecutive"),
    ("shorter than k readings", r"shorter than $k$ readings"),
    ("the cost K of", r"the cost $K$ of"),
    ("K the cost", r"$K$ the cost"),
    ("F the false alarms", r"$F$ the false alarms"),
    ("M the share", r"$M$ the share"),
    ("R the share", r"$R$ the share"),
    ("rate λ ", r"rate $\lambda$ "),
    ("λ the drops", r"$\lambda$ the drops"),
    ("for λ up to", r"for $\lambda$ up to"),
    ("a floor κ", r"a floor $\kappa$"),
    ("floor κ", r"floor $\kappa$"),
    ("margin δ ", r"margin $\delta$ "),
    ("margins δ ", r"margins $\delta$ "),
    ("waits k ", r"waits $k$ "),
    ("δ and k ", r"$\delta$ and $k$ "),
    ("the rank r ", r"the rank $r$ "),
    ("rank at most r ", r"rank at most $r$ "),
    ("as of t,", r"as of $t$,"),
    ("as of bucket t,", r"as of bucket $t$,"),
    ("the band at t is", r"the band at $t$ is"),
    ("at bucket t.", r"at bucket $t$."),
    ("is raised at t when", r"is raised at $t$ when"),
    ("ending at u is", r"ending at $u$ is"),
    ("ending at t has sum", r"ending at $t$ has sum"),
    ("the rows at t with", r"the rows at $t$ with"),
    ("below normal at t when", r"below normal at $t$ when"),
    ("first such t and ends at the first t at which", r"first such $t$ and ends at the first $t$ at which"),
    ("the decision at t is", r"the decision at $t$ is"),
    ("in L unless", r"in $L$ unless"),
    ("are in L:", r"are in $L$:"),
    ("empties L.", r"empties $L$."),
    ("of width w at", r"of width $w$ at"),
    ("of width w ending", r"of width $w$ ending"),
    ("w = 6 only", r"$w = 6$ only"),
    ("S_w(t) = \\sum", "S_w(t) = \\sum"),
]

GREEK = {"δ": r"\delta", "λ": r"\lambda", "τ": r"\tau", "κ": r"\kappa", "φ": r"\varphi", "σ": r"\sigma", "Δ": r"\Delta",
         "ℓ": r"\ell", "ρ": r"\rho"}
MATHSYM = {"≤": r"\le", "≥": r"\ge", "∈": r"\in", "⊂": r"\subset", "ℤ": r"\mathbb{Z}", "…": r"\dots", "−": "-", "·": r"\cdot",
           "×": r"\times", "±": r"\pm", "∃": r"\exists"}


def mathify(m):
    """Unicode inside a math segment -> LaTeX macros."""
    s = m
    for k, v in {**GREEK, **MATHSYM}.items():
        s = s.replace(k, v + " " if v.startswith("\\") and v[-1].isalpha() else v)
    return re.sub(r"\\([A-Za-z]+) (?=[^A-Za-z])", r"\\\1", s)


TOKEN = re.compile(r"(?<![\w\\/$.-])([A-Za-zℓρ][A-Za-z0-9]*(?:\^[-+A-Za-z0-9]|\^\{[^}]*\}|_\{[^}]*\}|_[A-Za-z0-9]+)+(?:\([^()]*\))?)(?!\w|\.\w)")
SETTING = re.compile(r"(?<!\$)(δ|k|w|r|m|n|p)\s=\s([0-9.]+)(?!\$)")
GREEKLONE = re.compile(r"(?<![\w$\\])([δλτκφσΔℓρ])(?![\w$])")


def inline_math(text):
    for a, b in PHRASES:
        segs = re.split(r"(\$[^$]*\$|`[^`]*`)", text)
        text = "".join(seg if i % 2 else seg.replace(a, b) for i, seg in enumerate(segs))
    # generic sub/superscript tokens, outside existing math and code
    parts = re.split(r"(\$[^$]*\$|`[^`]*`|\[[^\]]*\]\([^)]*\))", text)
    out = []
    for i, p in enumerate(parts):
        if i % 2 == 1:
            out.append(p)
            continue
        p = TOKEN.sub(lambda m: "$" + m.group(1) + "$", p)
        p = SETTING.sub(lambda m: "$" + m.group(1) + " = " + m.group(2) + "$", p)
        p = GREEKLONE.sub(lambda m: "$" + m.group(1) + "$", p)
        out.append(p)
    text = "".join(out)
    # convert unicode inside every math segment
    return re.sub(r"\$([^$]*)\$", lambda m: "$" + mathify(m.group(1)) + "$", text)


def esc(t):
    t = t.replace("\\", r"\textbackslash{}")
    for a, b in [("&", r"\&"), ("%", r"\%"), ("#", r"\#"), ("_", r"\_"), ("{", r"\{"), ("}", r"\}"), ("~", r"\textasciitilde{}"), ("^", r"\^{}")]:
        t = t.replace(a, b)
    t = t.replace("–", "--").replace("−", "--").replace("…", r"\dots{}").replace("×", r"$\times$").replace("·", r"$\cdot$").replace("∎", "")
    t = t.replace("≈", r"$\approx$").replace("—", "---")
    return t.replace("∈", r"$\in$").replace("≥", r"$\ge$").replace("≤", r"$\le$").replace("⊂", r"$\subset$").replace("ℤ", r"$\mathbb{Z}$")


def inline(text):
    """Markdown inline (code, links, bold, italic) + math -> LaTeX, escaping the prose."""
    text = inline_math(text)
    parts = re.split(r"(\$[^$]*\$|`[^`]*`|\[[^\]]*\]\([^)]*\)|https?://[^\s)]+|\*\*[^*]+\*\*|\*[^*]+\*)", text)
    out = []
    for p in parts:
        if not p:
            continue
        if p.startswith("$"):
            out.append(p)
        elif p.startswith("`"):
            # a long name may break after an underscore, a colon or a bracket
            code = re.sub(r"(\\_|:|\()", lambda m: m.group(1) + r"\allowbreak{}", esc(p[1:-1]))
            out.append(r"\texttt{" + code + "}")
        elif p.startswith("http"):
            out.append(r"\url{" + p + "}")
        elif p.startswith("["):
            m = re.match(r"\[([^\]]*)\]\(([^)]*)\)", p)
            out.append(r"\href{" + m.group(2) + "}{" + esc(m.group(1)) + "}")
        elif p.startswith("**"):
            out.append(r"\textbf{" + inline(p[2:-2]) + "}")
        elif p.startswith("*"):
            out.append(r"\emph{" + inline(p[1:-1]) + "}")
        else:
            out.append(esc(p))
    return "".join(out)


def table(lines):
    rows = [[c.strip() for c in l.strip().strip("|").split("|")] for l in lines]
    head, body = rows[0], rows[2:]
    n = len(head)
    longest = [max(len(r[i]) for r in rows if i < len(r)) for i in range(n)]
    size, sep = "\\small", ""
    slash = False
    heads = [inline(c) for c in head]
    if len(body) > 22:
        # a tall table: breaks across pages, small type, the columns sharing the width
        size, sep = "\\footnotesize", "\\setlength{\\tabcolsep}{2pt}"
        spec = ">{\\raggedright\\arraybackslash}p{0.17\\textwidth}" + ">{\\raggedright\\arraybackslash}X" * (n - 1)
        heads = [h.replace("/", "/\\allowbreak{}") for h in heads]
        out = ["{" + size + sep, "\\begin{xltabular}{\\textwidth}{" + spec + "}", "\\toprule", " & ".join(heads) + " \\\\", "\\midrule", "\\endhead"]
        for r in body:
            out.append(" & ".join(inline(c) for c in r) + " \\\\")
        out += ["\\bottomrule", "\\end{xltabular}}"]
        return "\n".join(out)
    if n >= 7:
        # a wide table: small type, a fixed first column, and the rest sharing the width by each one's longest word,
        # the piece that cannot wrap; headers and names wrap at a slash
        size, sep = "\\footnotesize", "\\setlength{\\tabcolsep}{2pt}"
        word = [max(len(w) for r in rows[:1] + body if i < len(r) for w in re.split(r"[\s/_:(]+", r[i].strip("`").replace(" / ", "+"))) for i in range(1, n)]
        share = [min(max(x + 1, 6), 13) for x in word]  # "0.16 / 0.31" counts as one word, and stays on one line
        spec = ">{\\raggedright\\arraybackslash}p{0.15\\textwidth}" + "".join(
            ">{\\hsize=%.3f\\hsize\\raggedright\\arraybackslash}X" % (x * (n - 1) / sum(share)) for x in share)
        env, begin = "tabularx", "\\begin{tabularx}{\\textwidth}{" + spec + "}"
        heads = [h.replace("/", "/\\allowbreak{}") for h in heads]
        slash = True
    elif sum(longest) > 70:
        spec = "".join(">{\\raggedright\\arraybackslash}X" if longest[i] > 14 else "l" for i in range(n))
        env, begin = "tabularx", "\\begin{tabularx}{\\textwidth}{" + spec + "}"
    else:
        env, begin = "tabular", "\\begin{tabular}{" + "l" * n + "}"
    out = ["\\begin{table}[H]\\centering" + size + sep, begin, "\\toprule", " & ".join(heads) + " \\\\", "\\midrule"]
    for r in body:
        cells = [inline(c) for c in r]
        if slash:  # a name such as weekday/weekend wraps at its slash, as the headers do
            cells = [c if "$" in c or "\\" in c else c.replace("/", "/\\allowbreak{}") for c in cells]
        out.append(" & ".join(cells) + " \\\\")
    out += ["\\bottomrule", "\\end{" + env + "}", "\\end{table}"]
    return "\n".join(out)


def wrap_code(line):
    """A long shell line in the reproducibility block: its comment goes on the next line, indented."""
    if len(line) > 90 and " # " in line:
        cmd, note = line.split(" # ", 1)
        return cmd.rstrip() + "\n    # " + note
    return line


FIGURES = {
    "diagram": "One judge, five widths: the recipe down the side, the machinery in the cells. A cell across the four count widths is machinery they share; the gap's column is where it differs.",
    "chart1": "Figure 1. False alarms per source-month against the share of drops to a tenth caught within the hour, for twelve bands at the 30-minute width; 14 real series, means per series. The dashed line joins the settings no other setting beats on both axes.",
    "chart2": "Figure 2. The false-alarm/missed-drop frontier over margins 0 to 40% and waits 1 to 4 readings, ratio band at the 30-minute width; 14 real series, means per series.",
}


def convert(md):
    lines = md.split("\n")
    out, i = [], 0
    while i < len(lines):
        l = lines[i]
        if l.startswith("# "):
            i += 1; continue  # title is set by \title
        if l.startswith("*2026"):
            i += 1; continue
        m = re.match(r"<\?claude block (\w+)\?>", l)
        if m:
            name = m.group(1)
            cap = FIGURES[name]
            out.append("\\begin{figure}[H]\\centering\\includegraphics[width=\\textwidth]{figs/" + name + ".png}\\caption*{" + inline(cap) + "}\\end{figure}")
            i += 1; continue
        if l.startswith("```"):
            lang = l[3:].strip()
            j = i + 1
            while not lines[j].startswith("```"):
                j += 1
            body = "\n".join(wrap_code(x) for x in lines[i + 1 : j])
            if lang == "latex":
                out.append("\\[\n" + body + "\n\\]")
            else:
                out.append("\\begin{footnotesize}\\begin{verbatim}\n" + body + "\n\\end{verbatim}\\end{footnotesize}")
            i = j + 1; continue
        if l.startswith("|"):
            j = i
            while j < len(lines) and lines[j].startswith("|"):
                j += 1
            out.append(table(lines[i:j])); i = j; continue
        if re.match(r"^\d+\. ", l):
            items = []
            while i < len(lines) and re.match(r"^\d+\. ", lines[i]):
                items.append(re.sub(r"^\d+\. ", "", lines[i])); i += 1
            out.append("\\begin{enumerate}\n" + "\n".join("\\item " + inline(x) for x in items) + "\n\\end{enumerate}"); continue
        if l.startswith("- "):
            items = []
            while i < len(lines) and lines[i].startswith("- "):
                items.append(lines[i][2:]); i += 1
            if out and out[-1].startswith("\\section*{References"):
                out.append("\\begin{itemize}\\setlength{\\itemsep}{2pt}\n" + "\n".join("\\item " + inline(x) for x in items) + "\n\\end{itemize}")
            else:
                out.append("\\begin{itemize}\n" + "\n".join("\\item " + inline(x) for x in items) + "\n\\end{itemize}")
            continue
        m = re.match(r"^(#+) (.*)", l)
        if m:
            level, title = len(m.group(1)), m.group(2)
            title = re.sub(r"^\d+(\.\d+)?\.? ", "", title)
            if title in ("Abstract",):
                out.append("ABSTRACT_MARKER")
            elif title == "References":
                out.append("\\section*{References}")
            elif title.startswith("Appendix"):
                out.append("\\section*{" + inline(title) + "}")
            elif level == 2:
                out.append("\\section{" + inline(title) + "}")
            else:
                out.append("\\subsection{" + inline(title) + "}")
            i += 1; continue
        if l.strip() == "":
            i += 1; continue
        # paragraph (one line each in this file)
        if l.startswith("**Proposition 1.**"):
            out.append("\\begin{proposition}" + inline(l[len("**Proposition 1.**"):]) + "\n")
            # the display and proof follow; close after the display
            out.append("PROP_OPEN")
        elif l.startswith("**Corollary 2.**"):
            out.append("\\begin{corollary}" + inline(l[len("**Corollary 2.**"):]) + "\\end{corollary}")
        elif l.startswith("*Proof.*"):
            out.append("\\begin{proof}" + inline(l[len("*Proof.*"):]).replace("\\end{proof}", "") + "\\end{proof}")
        else:
            out.append(inline(l))
        i += 1
    text = "\n\n".join(out)
    # close the proposition after its display
    text = re.sub(r"PROP_OPEN\n\n(\\\[\n.*?\n\\\])", r"\1\n\\end{proposition}", text, flags=re.S)
    # abstract: the paragraph after the marker
    text = re.sub(r"ABSTRACT_MARKER\n\n(.*?)\n\n", r"\\begin{abstract}\n\1\n\\end{abstract}\n\n", text, count=1, flags=re.S)
    return text


body = convert(src)
title = re.search(r"^# (.*)$", src, re.M).group(1)
PRE = r"""\documentclass[11pt]{article}
\usepackage[a4paper,margin=1in]{geometry}
\usepackage{amsmath,amssymb,amsthm}
\usepackage{booktabs,tabularx,array,float,xltabular}
\usepackage{graphicx,caption}
\usepackage{microtype}
\usepackage[T1]{fontenc}
\usepackage{lmodern}

\usepackage[hidelinks]{hyperref}
\urlstyle{same}
\newtheorem{proposition}{Proposition}
\newtheorem{corollary}[proposition]{Corollary}
\setlength{\parskip}{4pt}
\title{""" + esc(title) + r"""}
\author{John Pavlik \and Claude (Anthropic)}
\date{November 9, 2026}
\begin{document}
\maketitle
"""
open("paper.tex", "w", encoding="utf-8").write(PRE + body + "\n\\end{document}\n")
left = re.findall(r"[^\x00-\x7f]", body)
import collections
print("non-ascii left:", collections.Counter(left).most_common())
