 - dist plots remove title
 - author-level analysis with pairwise diversity?
 
 
 ## PAPER
 
 need to decide how i interpret my results.
 
 - background and related work
    - metrics (formulas, intuitions)
    - superstar statistics -- superstars perform better on these metrics
    - group-level statistics (we don't replicate this)
    - statistics at author level (we replicate this)
    - statistics for early innovators and early collaborators over their careers (we replicate only citations per publication over time)

 
- mabye dataset stats/metadata into the appendix?
- link github
- old innovation stats into the abstract, basically a fun fact 
 - write about:
    - new metric. mention highest effect size. 
    - statistical tests between the metrics.
    - interpretation:
        - t-t0: early innovators cited less than collaborators, but the definitions are arbitrary, and the 'interesting' part with the gap widening remains
            - critique possible: early innovators are selected for not working with superstars, vs collaborators which are exactly the opposite. so of course collaborators are hit more by removing superstar papers! kelty et al argue that this is free-riding on the success of others, but a paper having a superstar author is also a signal of that paper being good, so we are disproportionally removing their 'good' papers. 
        - author-level: authors who cite superstars more produce more diverse research, but their research is less innovative. there's also a clear benefit to citing superstars more, as authors who cite more superstars get many more citations, more than 10x between 0-20% vs 80-100%. also: impact of excluding superstar papers much smaller than in kelty et al. unclear why.
    - data processing mistake: should have picked a subset of AUTHORS, kept only cs papers, because dropping 10% of papers randomly left many authors with just 1 or 2 papers, skewing measures of e.g. diversity. mention this migth explain high effect size for pairwise-diveristy.
    - things ommitted (group-level analysis, t-t0 plot but with innovation, redudandancy metric)