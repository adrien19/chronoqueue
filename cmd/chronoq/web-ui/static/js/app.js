document.addEventListener('click', (e) => {
  if (!(e.target instanceof Element)) return;
  const el = e.target.closest('[data-copy]');
  if (!el) return;
  if (!navigator.clipboard) return;
  navigator.clipboard.writeText(el.getAttribute('data-copy') || '').catch(() => { });
});
