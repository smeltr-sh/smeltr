# One judge, five widths: the recipe down the side (rows), the machinery in the cells (columns).
# A cell spanning columns is machinery shared; a split cell is where the widths differ.
import html
W=760; X0=24; LAB=118; CW=108; GW=172; TOP=92; RH=58
cols=[X0+LAB+i*CW for i in range(4)]; gx=X0+LAB+4*CW; right=gx+GW
rows=[
 ("Statistic", [((0,4),["rows in the window,","S_w(t)"]),((4,5),["empty buckets since","the last row, g(t)"])]),
 ("Typical level", [((0,4),["median of the same window at this","time of day over the past 7 days;","the reading is the ratio to it"]),((4,5),["none: all spells pooled,","the trade measured in 6.8"])]),
 ("History", [((0,2),["7 days of windows"]),((2,4),["28 days of windows"]),((4,5),["a year of","completed spells"])]),
 ("Quantile", [((0,2),["0.05% of the ratios: between","the 2nd and 3rd lowest of 2,000"]),((2,4),["0.05% of the ratios: between","the 5th and 6th lowest of 8,000"]),((4,5),["the maximum; in the first","year, plus an allowance"])]),
 ("Margin", [((0,4),["20% below it"]),((4,5),["50% beyond it"])]),
 ("Wait", [((0,4),["4 readings in a row"]),((4,5),["1 reading"])]),
 ("Judged where", [((0,4),["the typical window holds 10 rows"]),((4,5),["28 days watched and","a spell completed"])]),
 ("Not learned", [((0,4),["the buckets under a count alarm, one set for the four"]),((4,5),["the spells under","a gap alarm"])]),
 ("Alarm ends", [((0,4),["on a reading inside the band"]),((4,5),["at the first row"])]),
 ("Alarm", [((0,5),["whichever width fires first"])]),
]
def svg(ink,quiet,edge,accent,jsx):
    o=[]; A=lambda s:o.append(s)
    H=TOP+len(rows)*RH+66
    q=lambda s: html.escape(s,quote=True)
    A(f"<svg viewBox='0 0 {W} {H}' role='img' aria-label='One judge at five widths: the recipe down the side, the machinery in the cells' fontSize='13'>")
    A(f"<text x='{X0}' y='32' fontSize='15' fontWeight='600' fill='{ink}'>One judge, five widths: the recipe down the side, the machinery in the cells</text>")
    # column headers
    A(f"<text x='{(cols[0]+cols[3]+CW)//2}' y='{TOP-26}' textAnchor='middle' fontSize='11.5' fill='{quiet}'>four count widths: rows in the last</text>")
    for x,l in zip(cols,["5 min","30 min","6 h","1 day"]): A(f"<text x='{x+CW//2}' y='{TOP-10}' textAnchor='middle' fontWeight='600' fill='{ink}'>{l}</text>")
    A(f"<text x='{gx+GW//2}' y='{TOP-26}' textAnchor='middle' fontSize='11.5' fill='{quiet}'>the fifth width: time</text>")
    A(f"<text x='{gx+GW//2}' y='{TOP-10}' textAnchor='middle' fontWeight='600' fill='{ink}'>the gap</text>")
    for r,(lab,cells) in enumerate(rows):
        y=TOP+r*RH
        A(f"<text x='{X0}' y='{y+RH//2+4}' fontWeight='600' fill='{ink}'>{q(lab)}</text>")
        for (a,b),lines in cells:
            x1=(cols+[gx,right])[a]; x2=(cols+[gx,right])[b]
            shared = (b-a)>=4
            A(f"<rect x='{x1+3}' y='{y+3}' width='{x2-x1-6}' height='{RH-6}' rx='6' fill='{accent}' fillOpacity='{0.10 if shared else 0}' stroke='{accent if shared else edge}' strokeWidth='{1.5 if shared else 1.1}'/>")
            n=len(lines); fs=11.5
            for i,t in enumerate(lines):
                ty=y+RH//2+4+(i-(n-1)/2)*14
                A(f"<text x='{(x1+x2)//2}' y='{ty:.0f}' textAnchor='middle' fontSize='{fs}' fill='{ink if shared else quiet}'>{q(t)}</text>")
    # recipe arrow down the side
    ax=X0+LAB-14; A(f"<path d='M{ax} {TOP+6}V{TOP+len(rows)*RH-6}' stroke='{edge}' strokeWidth='1.25' fill='none' markerEnd='url(#rec-arrow)'/>")
    A(f"<text x='{X0}' y='{H-40}' fontSize='11.5' fill='{quiet}'>A cell across the four count widths is machinery they share; the gap's column is where it differs.</text>")
    A(f"<text x='{X0}' y='{H-24}' fontSize='11.5' fill='{quiet}'>Under exchangeability a count edge is undercut at most (r + 1)/(n + 1) of the time, about 3 in 2,017 at the short widths;</text>")
    A(f"<text x='{X0}' y='{H-8}' fontSize='11.5' fill='{quiet}'>the gap's edge about once a year at most (Section 2.8).</text>")
    A("</svg>")
    s="\n".join(o)
    s=s.replace("<svg ",f"<svg xmlns='http://www.w3.org/2000/svg' ",1) if not jsx else s
    defs=f"<defs><marker id='rec-arrow' viewBox='0 0 10 10' refX='9' refY='5' markerWidth='6' markerHeight='6' orient='auto-start-reverse'><path d='M0 0L10 5L0 10z' fill='{edge}'/></marker></defs>"
    s=s.replace("fontSize='13'>","fontSize='13'>"+defs,1)
    if not jsx:
        for a,b in [("fontSize","font-size"),("fontWeight","font-weight"),("textAnchor","text-anchor"),("fillOpacity","fill-opacity"),("strokeWidth","stroke-width"),("markerEnd","marker-end"),("markerWidth","marker-width"),("markerHeight","marker-height")]: s=s.replace(a,b)
    return s
open('figs/diagram.svg','w').write(svg('#1d1f22','#5d6168','#9a9a9a','#1f5a8a',False))
j=svg("{ink}","{quiet}","{edge}","{accent}",True).replace("'{ink}'","{ink}").replace("'{quiet}'","{quiet}").replace("'{edge}'","{edge}").replace("'{accent}'","{accent}")
open('diagram_code.js','w').write("export default () => { const edge = 'var(--cds-chart-axis)', accent = 'var(--cds-chart-categorical-1)', ink = 'var(--cds-text-primary)', quiet = 'var(--cds-text-secondary)'; return "+j.replace("\n","")+"; };")
print('ok')
