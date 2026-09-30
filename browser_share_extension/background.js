const CONTENT_SCRIPT_ID = "telegram-page-sender";

function originPattern(rawUrl) {
  const url = new URL(rawUrl);
  if (!['http:', 'https:'].includes(url.protocol)) {
    throw new Error('Разрешены только http/https адреса');
  }
  return `${url.protocol}//${url.host}/*`;
}

async function configureInjection() {
  const { pageUrlPrefix } = await chrome.storage.local.get('pageUrlPrefix');
  const registered = await chrome.scripting.getRegisteredContentScripts({
    ids: [CONTENT_SCRIPT_ID],
  });
  if (registered.length) {
    await chrome.scripting.unregisterContentScripts({ ids: [CONTENT_SCRIPT_ID] });
  }
  if (!pageUrlPrefix) {
    return;
  }

  const matches = [originPattern(pageUrlPrefix)];
  const allowed = await chrome.permissions.contains({ origins: matches });
  if (!allowed) {
    return;
  }
  await chrome.scripting.registerContentScripts([{
    id: CONTENT_SCRIPT_ID,
    matches,
    js: ['content.js'],
    runAt: 'document_idle',
    persistAcrossSessions: true,
  }]);
}

async function sendPage(message, sender) {
  const settings = await chrome.storage.local.get([
    'pageUrlPrefix',
    'relayUrl',
    'relaySecret',
  ]);
  if (!settings.relayUrl || !settings.relaySecret) {
    throw new Error('Сначала заполните адрес relay и секрет в настройках расширения');
  }

  const senderUrl = sender.tab?.url || sender.url || '';
  let allowedPage = false;
  try {
    const configured = new URL(settings.pageUrlPrefix);
    const current = new URL(senderUrl);
    allowedPage = current.origin === configured.origin
      && current.pathname === configured.pathname;
  } catch (_) {
    allowedPage = false;
  }
  if (!allowedPage) {
    throw new Error('Эта страница не соответствует настроенному адресу');
  }

  const response = await fetch(settings.relayUrl, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${settings.relaySecret}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      url: senderUrl,
      title: message.payload.title,
      text: message.payload.text,
    }),
  });

  let result = {};
  try {
    result = await response.json();
  } catch (_) {
    result = {};
  }
  if (!response.ok || !result.ok) {
    throw new Error(result.error || `Relay вернул HTTP ${response.status}`);
  }
  return result;
}

chrome.runtime.onInstalled.addListener(({ reason }) => {
  configureInjection().catch(() => {});
  if (reason === 'install') {
    chrome.runtime.openOptionsPage();
  }
});

chrome.runtime.onStartup.addListener(() => {
  configureInjection().catch(() => {});
});

chrome.action.onClicked.addListener(() => {
  chrome.runtime.openOptionsPage();
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message?.type === 'configure') {
    configureInjection()
      .then(() => sendResponse({ ok: true }))
      .catch((error) => sendResponse({ ok: false, error: error.message }));
    return true;
  }
  if (message?.type === 'share-page') {
    sendPage(message, sender)
      .then((result) => sendResponse(result))
      .catch((error) => sendResponse({ ok: false, error: error.message }));
    return true;
  }
  return false;
});
