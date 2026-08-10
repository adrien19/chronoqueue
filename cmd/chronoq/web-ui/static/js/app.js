document.addEventListener('click', (e) => {
  if (!(e.target instanceof Element)) return;
  const el = e.target.closest('[data-copy]');
  if (!el) return;
  if (!navigator.clipboard) return;
  navigator.clipboard.writeText(el.getAttribute('data-copy') || '').catch(() => { });
});

document.addEventListener('submit', (e) => {
  const form = e.target;
  if (!(form instanceof HTMLFormElement)) return;
  const message = form.getAttribute('data-confirm');
  if (message && !window.confirm(message)) e.preventDefault();
});

const queueType = document.getElementById('queue_type');
if (queueType) {
  const toggleExclusivityKey = () => {
    const section = document.getElementById('exclusivity_key_section');
    const input = document.getElementById('exclusivity_key');
    if (!section || !(input instanceof HTMLInputElement)) return;
    const exclusive = queueType.value === 'exclusive';
    section.classList.toggle('hidden', !exclusive);
    input.required = exclusive;
    if (!exclusive) input.value = '';
  };
  queueType.addEventListener('change', toggleExclusivityKey);
  toggleExclusivityKey();
}

const scheduleType = document.getElementById('schedule_type');
if (scheduleType) {
  const toggleScheduleType = () => {
    const cronSection = document.getElementById('cron_section');
    const calendarSection = document.getElementById('calendar_section');
    const cronExpression = document.getElementById('cron_expression');
    const timezone = document.getElementById('timezone');
    if (!cronSection || !calendarSection || !(cronExpression instanceof HTMLInputElement) || !(timezone instanceof HTMLInputElement)) return;
    const cron = scheduleType.value === 'cron';
    cronSection.classList.toggle('hidden', !cron);
    calendarSection.classList.toggle('hidden', cron);
    cronExpression.required = cron;
    timezone.required = !cron;
  };
  scheduleType.addEventListener('change', toggleScheduleType);
  toggleScheduleType();
}
