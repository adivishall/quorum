# site/ — the project website

A static site that describes Quorum: plain HTML and CSS, no JavaScript, no build step, no
dependencies. It is documentation, not the database: nothing here runs a node, and the page
never pretends to.

## Preview locally

```bash
python3 -m http.server -d site 8000
# then open http://localhost:8000/
```

## How it is published

- **GitHub Pages** — `.github/workflows/pages.yml` uploads this directory as the Pages artifact
  on every push to `main` that touches `site/`. Nothing outside `site/` is published.
- **Vercel (Hobby)** — import the repository, set the project's *Root Directory* to `site`,
  framework preset *Other*, and leave the build command empty. `vercel.json` here pins that
  (`framework: null`) and adds security headers. Both hosts serve the same files; every link in
  the pages is relative so the site works at `/` and under a sub-path.

## Keeping it honest

Every claim links to a file on the repository's `main` branch. When the system changes, change
the page: `index.html` names the commit it describes in its footer, and the recorded transcript
in it was produced by `demo.sh`'s commands against binaries built from that commit.
