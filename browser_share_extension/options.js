const form = document.querySelector('#settings-form');
const pageUrlPrefixInput = document.querySelector('#page-url-prefix');
const relayUrlInput = document.querySelector('#relay-url');
const relaySecretInput = document.querySelector('#relay-secret');
const defaultTextInput = document.querySelector('#default-text');
const status = document.querySelector('#status');
const submitButton = form.querySelector('button[type="submit"]');

function originPattern(rawUrl) {
  const url = new URL(rawUrl);
  if (!['http:', 'https:'].includes(url.protocol)) {
    throw new Error('Разрешены только http/https адреса');
  }
  return `${url.protocol}//${url.host}/*`;
}

function setStatus(message, kind = '') {
  status.textContent = message;
  status.className = kind;
}

async function loadSettings() {
  const settings = await chrome.storage.local.get([
    'pageUrlPrefix',
    'relayUrl',
    'relaySecret',
    'defaultText',
  ]);
  pageUrlPrefixInput.value = settings.pageUrlPrefix || 'https://fundoor.pro/spreadchart';
  relayUrlInput.value = settings.relayUrl || '';
  relaySecretInput.value = settings.relaySecret || '';
  defaultTextInput.value = settings.defaultText || '';
}

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  submitButton.disabled = true;
  setStatus('Сохраняю...');

  try {
    const pageUrlPrefix = pageUrlPrefixInput.value.trim();
    const relayUrl = relayUrlInput.value.trim();
    const relaySecret = relaySecretInput.value.trim();
    const defaultText = defaultTextInput.value.trim();
    if (relaySecret.length < 24) {
      throw new Error('Relay secret должен быть не короче 24 символов');
    }

    const origins = [...new Set([
      originPattern(pageUrlPrefix),
      originPattern(relayUrl),
    ])];
    const granted = await chrome.permissions.request({ origins });
    if (!granted) {
      throw new Error('Без доступа к странице и relay расширение работать не сможет');
    }

    await chrome.storage.local.set({
      pageUrlPrefix,
      relayUrl,
      relaySecret,
      defaultText,
    });
    const response = await chrome.runtime.sendMessage({ type: 'configure' });
    if (!response?.ok) {
      throw new Error(response?.error || 'Не удалось зарегистрировать кнопку');
    }
    setStatus('Сохранено. Обновите целевую страницу.', 'success');
  } catch (error) {
    setStatus(error.message, 'error');
  } finally {
    submitButton.disabled = false;
  }
});

loadSettings().catch((error) => setStatus(error.message, 'error'));
