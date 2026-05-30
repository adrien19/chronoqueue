import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const tailwindcss = require('tailwindcss');

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..');
const srcFile = path.join(root, 'static', 'css', 'input.css');
const outFile = path.join(root, 'static', 'css', 'app.css');
const watch = process.argv.includes('--watch');

function walk(dir, exts = new Set(['.gohtml', '.html', '.js'])) {
    const files = [];
    if (!fs.existsSync(dir)) return files;
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        const full = path.join(dir, entry.name);
        if (entry.isDirectory()) files.push(...walk(full, exts));
        else if (exts.has(path.extname(entry.name))) files.push(full);
    }
    return files;
}

function collectCandidates(content) {
    const classes = new Set();
    const regexes = [
        /class="([^"]+)"/g,
        /class='([^']+)'/g,
        /class:\s*"([^"]+)"/g,
    ];
    for (const regex of regexes) {
        let match;
        while ((match = regex.exec(content))) {
            match[1].split(/\s+/).filter(Boolean).forEach((c) => classes.add(c));
        }
    }
    return classes;
}

async function build() {
    const source = fs.readFileSync(srcFile, 'utf8');
    const files = [
        ...walk(path.join(root, 'templates')),
        ...walk(path.join(root, 'static', 'js')),
    ];

    // Seed with known dynamic classes generated from Go handlers
    const candidates = new Set([
        'cq-badge', 'cq-badge-good', 'cq-badge-warn', 'cq-badge-muted',
        'border-red-500/25', 'bg-red-500/10', 'text-red-300',
        'border-sky-500/25', 'bg-sky-500/10', 'text-sky-300',
        'text-emerald-300', 'text-amber-300', 'text-zinc-200',
    ]);
    for (const file of files) {
        const content = fs.readFileSync(file, 'utf8');
        for (const c of collectCandidates(content)) candidates.add(c);
    }

    const compiled = await tailwindcss.compile(source, {
        from: srcFile,
        loadStylesheet: async (id, base) => {
            const twRoot = path.dirname(require.resolve('tailwindcss/package.json'));
            if (id === 'tailwindcss') {
                const cssPath = path.join(twRoot, 'index.css');
                return { base: twRoot, content: fs.readFileSync(cssPath, 'utf8') };
            }
            const target = path.isAbsolute(id) ? id : path.join(base || root, id);
            return { base: path.dirname(target), content: fs.readFileSync(target, 'utf8') };
        },
    });

    const css = compiled.build([...candidates]);
    fs.mkdirSync(path.dirname(outFile), { recursive: true });
    fs.writeFileSync(outFile, css, 'utf8');
    console.log(`built ${path.relative(root, outFile)} with ${candidates.size} candidates`);
}

await build();

if (watch) {
    const watchDirs = [
        path.join(root, 'templates'),
        path.join(root, 'static', 'js'),
        path.join(root, 'static', 'css'),
    ];
    for (const dir of watchDirs) {
        if (fs.existsSync(dir)) {
            fs.watch(dir, { recursive: true }, async () => {
                try { await build(); } catch (err) { console.error(err); }
            });
        }
    }
    console.log('watching for changes...');
}
