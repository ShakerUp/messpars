(() => {
  if (window.__telegramPageSenderLoaded) {
    return;
  }
  window.__telegramPageSenderLoaded = true;

  const HOST_ID = 'telegram-page-sender-root';
  let settings = null;

  const styles = `
    :host {
      all: initial;
      display: inline-flex;
      align-items: center;
      flex: 0 0 auto;
      align-self: center;
      margin-inline-start: 20px;
      z-index: 2147483647;
    }
    * { box-sizing: border-box; }
    .tps-launcher {
      display: inline-flex;
      height: 34px;
      align-items: center;
      gap: 7px;
      padding: 0 12px;
      border: 1px solid #2e5380;
      border-radius: 6px;
      background: #101a2a;
      color: #dbeafa;
      cursor: pointer;
      white-space: nowrap;
      font: 650 13px/1 system-ui, sans-serif;
    }
    .tps-launcher:hover { border-color: #54a8ef; background: #172b46; }
    .tps-icon { color: #54b5f8; font-size: 18px; line-height: 1; }
    .tps-panel {
      position: fixed;
      z-index: 2147483647;
      display: none;
      width: min(360px, calc(100vw - 24px));
      padding: 14px;
      border: 1px solid #39434d;
      border-radius: 8px;
      background: #171c22;
      color: #edf2f6;
      box-shadow: 0 14px 40px rgba(0, 0, 0, 0.45);
      font: 14px/1.4 system-ui, sans-serif;
    }
    .tps-panel.open { display: block; }
    .tps-title {
      overflow: hidden;
      margin: 0 0 10px;
      color: #aeb9c4;
      font-size: 12px;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .tps-text {
      display: block;
      width: 100%;
      min-height: 100px;
      padding: 10px;
      border: 1px solid #3a4651;
      border-radius: 6px;
      outline: none;
      resize: vertical;
      background: #0f1317;
      color: #f2f5f7;
      font: 14px/1.45 system-ui, sans-serif;
    }
    .tps-text:focus {
      border-color: #2aabee;
      box-shadow: 0 0 0 2px rgba(42, 171, 238, 0.18);
    }
    .tps-footer {
      display: flex;
      min-height: 36px;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-top: 10px;
    }
    .tps-status {
      overflow: hidden;
      color: #95a2ae;
      font-size: 12px;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .tps-status.error { color: #ff8585; }
    .tps-status.success { color: #78d39a; }
    .tps-actions { display: flex; gap: 7px; }
    .tps-button {
      height: 34px;
      padding: 0 12px;
      border: 1px solid #3a4651;
      border-radius: 6px;
      background: #222931;
      color: #dfe6eb;
      cursor: pointer;
      font: 650 13px/1 system-ui, sans-serif;
    }
    .tps-button.primary {
      border-color: #2aabee;
      background: #2aabee;
      color: #06151d;
    }
    .tps-button:disabled { cursor: wait; opacity: 0.65; }
  `;

  function isAllowedPage() {
    if (!settings?.pageUrlPrefix) {
      return false;
    }
    try {
      const configured = new URL(settings.pageUrlPrefix);
      const current = new URL(location.href);
      return current.origin === configured.origin
        && current.pathname === configured.pathname;
    } catch (_) {
      return false;
    }
  }

  function removeUi() {
    document.getElementById(HOST_ID)?.remove();
  }

  function findSocketAnchor() {
    const anchor = document.querySelector('#chartHeaderBar .chb-socket');
    return /fundoor socket/i.test(anchor?.querySelector('.chb-col-label')?.textContent || '')
      ? anchor
      : null;
  }

  function createUi(anchor) {
    if (document.getElementById(HOST_ID) || !isAllowedPage()) {
      return;
    }

    const host = document.createElement('div');
    host.id = HOST_ID;
    const shadow = host.attachShadow({ mode: 'closed' });
    shadow.innerHTML = `
      <style>${styles}</style>
      <button class="tps-launcher" type="button" title="Отправить текущую страницу в Telegram"><span class="tps-icon" aria-hidden="true">➤</span>В Telegram</button>
      <section class="tps-panel" aria-label="Отправка страницы в Telegram">
        <p class="tps-title"></p>
        <textarea class="tps-text" maxlength="2500" aria-label="Текст сообщения"></textarea>
        <div class="tps-footer">
          <span class="tps-status" role="status"></span>
          <div class="tps-actions">
            <button class="tps-button cancel" type="button">Отмена</button>
            <button class="tps-button primary send" type="button">Отправить</button>
          </div>
        </div>
      </section>
    `;
    anchor.insertAdjacentElement('afterend', host);

    const launcher = shadow.querySelector('.tps-launcher');
    const panel = shadow.querySelector('.tps-panel');
    const title = shadow.querySelector('.tps-title');
    const textarea = shadow.querySelector('.tps-text');
    const status = shadow.querySelector('.tps-status');
    const cancel = shadow.querySelector('.cancel');
    const send = shadow.querySelector('.send');

    const setStatus = (message, kind = '') => {
      status.textContent = message;
      status.className = `tps-status ${kind}`.trim();
    };
    const close = () => {
      panel.classList.remove('open');
      setStatus('');
    };

    launcher.addEventListener('click', () => {
      title.textContent = document.title || location.hostname;
      textarea.value = settings.defaultText || '';
      panel.classList.toggle('open');
      setStatus('');
      if (panel.classList.contains('open')) {
        const buttonRect = launcher.getBoundingClientRect();
        const panelWidth = Math.min(360, window.innerWidth - 24);
        panel.style.left = `${Math.max(12, Math.min(buttonRect.left, window.innerWidth - panelWidth - 12))}px`;
        panel.style.top = `${Math.min(buttonRect.bottom + 8, window.innerHeight - 190)}px`;
        textarea.focus();
      }
    });
    cancel.addEventListener('click', close);
    send.addEventListener('click', async () => {
      send.disabled = true;
      setStatus('Отправляю...');
      try {
        const response = await chrome.runtime.sendMessage({
          type: 'share-page',
          payload: {
            url: location.href,
            title: document.title,
            text: textarea.value.trim(),
          },
        });
        if (!response?.ok) {
          throw new Error(response?.error || 'Неизвестная ошибка');
        }
        setStatus('Отправлено', 'success');
        setTimeout(close, 900);
      } catch (error) {
        setStatus(error.message, 'error');
      } finally {
        send.disabled = false;
      }
    });
  }

  function syncUi() {
    const host = document.getElementById(HOST_ID);
    if (!isAllowedPage()) {
      removeUi();
      return;
    }

    const anchor = findSocketAnchor();
    if (host && host.previousElementSibling !== anchor) {
      removeUi();
    }
    if (anchor) {
      createUi(anchor);
    }
  }

  chrome.storage.local.get(['pageUrlPrefix', 'defaultText']).then((stored) => {
    settings = stored;
    syncUi();
    window.addEventListener('popstate', syncUi);
    window.addEventListener('hashchange', syncUi);
    setInterval(syncUi, 1000);
  });
})();
