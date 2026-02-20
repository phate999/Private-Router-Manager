/**
 * Private Router Manager - Vanilla JavaScript
 * Routers JSON editor and deployment UI
 */

(function () {
  'use strict';

  const PAGINATION_OPTS = [25, 50, 100, 200, 500];
  const ROUTER_BASE_KEYS = ['state', 'ip_address', 'username', 'password', 'port', 'created_at'];
  const DEFAULT_COLUMNS = ['State', 'IP Address', 'Hostname', 'Description', 'MAC', 'Serial Number', 'Asset ID', 'Product Name', 'NCOS Version', 'Username', 'Password', 'Port', 'Created At'];
  const COLUMN_DEFAULT_PATHS = {
    hostname: 'config.system.system_id',
    mac: 'status.product_info.mac0',
    serial_number: 'status.product_info.manufacturing.serial_num',
    product_name: 'status.product_info.product_name',
    ncos_version: 'status.fw_info',
    description: 'config.system.desc',
    asset_id: 'config.system.asset_id',
  };
  const CREDENTIAL_FIELDS = ['username', 'password', 'port'];
  const READONLY_FIELDS = new Set(['created_at']);
  const PATH_EXCLUDED_FIELDS = new Set(['state', 'ip_address']);
  const DEFAULT_COL_WIDTHS = {
    state: 72,
    ip_address: 130,
    hostname: 140,
    description: 160,
    mac: 145,
    serial_number: 160,
    asset_id: 120,
    product_name: 140,
    ncos_version: 200,
    username: 100,
    password: 100,
    port: 90,
  };
  const HEADER_CHROME_PX = 48;  // space for sort arrow + resizer affordance
  const ACRONYMS = new Set(['ip', 'mac', 'ncos', 'nc']);

  function normalizeColumnName(displayName) {
    return String(displayName || '').trim().toLowerCase().replace(/\s+/g, '_') || '';
  }

  // Resolve dot+bracket paths: "status.wan.devices[0].diagnostics.RSRP" or "status.wan.devices[-1]...."
  function getValueByPath(obj, pathStr) {
    if (!obj || pathStr == null || pathStr === '') return undefined;
    const path = String(pathStr).trim();
    if (!path) return undefined;
    const parts = [];
    let cur = '';
    let inBracket = false;
    for (let i = 0; i < path.length; i++) {
      const c = path[i];
      if (c === '[') {
        if (cur) { parts.push(cur); cur = ''; }
        inBracket = true;
      } else if (c === ']') {
        if (inBracket && cur !== '') parts.push(isNaN(Number(cur)) ? cur : parseInt(cur, 10));
        cur = ''; inBracket = false;
      } else if (inBracket) {
        cur += c;
      } else if (c === '.') {
        if (cur) { parts.push(cur); cur = ''; }
      } else {
        cur += c;
      }
    }
    if (cur) parts.push(cur);
    let v = obj;
    for (const p of parts) {
      if (v == null) return undefined;
      v = v[p];
    }
    return v;
  }

  function toDotBracketPath(path) {
    if (!path || typeof path !== 'string') return path;
    const s = path.trim();
    if (!s) return s;
    // Normalize slash to dot for consistent traversal; support status/lan/[0] and status.lan.[0]
    return s.replace(/\//g, '.').replace(/\|/g, '.');
  }

  function prettyPrintFieldName(fieldKey) {
    return String(fieldKey || '')
      .split('_')
      .map(part => ACRONYMS.has(part.toLowerCase()) ? part.toUpperCase() : (part.charAt(0).toUpperCase() + part.slice(1).toLowerCase()))
      .join(' ');
  }

  function normalizeRouterForSave(r) {
    const out = {};
    const allKeys = new Set([...ROUTER_BASE_KEYS, ...getColumnFieldKeys()]);
    allKeys.forEach(f => {
      if (!(f in r) && !ROUTER_BASE_KEYS.includes(f)) return;
      if (f === 'port') {
        const v = r[f];
        out[f] = (typeof v === 'number' ? v : parseInt(v, 10)) || 8080;
      } else if (f === 'username') {
        out[f] = String(r[f] ?? '').trim() || 'admin';
      } else {
        out[f] = String(r[f] ?? '').trim();
      }
    });
    Object.keys(r).forEach(k => {
      if (!allKeys.has(k) && r[k] != null && r[k] !== '') {
        if (k === 'column_paths') {
          if (typeof r[k] === 'object' && r[k] !== null && !Array.isArray(r[k])) out[k] = r[k];
        } else out[k] = r[k];
      }
    });
    return out;
  }

  function sanitizeRouterColumnPaths(r) {
    const cp = r?.column_paths;
    if (cp != null && (typeof cp !== 'object' || Array.isArray(cp) || (typeof cp === 'string' && cp === '[object Object]'))) {
      delete r.column_paths;
    }
    return r;
  }

  function getColumnFieldKeys() {
    const cols = state.columns && state.columns.length ? state.columns : DEFAULT_COLUMNS;
    return cols.map(c => normalizeColumnName(c)).filter(k => k && !PATH_EXCLUDED_FIELDS.has(k));
  }

  function getPathForRouter(router, field) {
    const override = router?.column_paths && typeof router.column_paths === 'object' ? router.column_paths[field] : undefined;
    return override != null && override !== '' ? String(override) : (state.columnDefaultPaths[field] || null);
  }

  const state = {
    routers: [],
    columns: [],
    columnDefaultPaths: {},
    routerFileLocked: false,
    connectionTimeout: 2,
    connectionRetries: 1,
    maxWorkers: 64,
    maxWorkersFormula: 'sqrt',
    maxWorkersPerCpu: 4,
    useAsyncClient: false,
    deployType: 'licenses',
    lastFile: '',
    routersSort: { primary: 'ip_address', primaryDir: 1, secondary: null, secondaryDir: 1 },
    routersPage: 0,
    routersPerPage: 100,
    pingResults: [],
    pingOfflineSince: {},
    pingSort: { primary: 'status', primaryDir: 1, secondary: 'loss_pct', secondaryDir: 1 },
    pingPage: 0,
    pingPerPage: 100,
    routersRowSelected: new Set(),
    routersRowExpanded: null,
    routersColumnWidths: {},
    columnDisplayNames: {},
    routerWanData: {},
    remoteApiPage: 0,
    remoteApiPerPage: 100,
  };

  function createPaginationBar(idPrefix, totalItems, page, perPage, onPageChange, onPerPageChange) {
    const totalPages = Math.max(1, Math.ceil(totalItems / perPage));
    const curPage = Math.min(page, totalPages - 1);
    const start = curPage * perPage;
    const end = Math.min(start + perPage, totalItems);
    const bar = document.createElement('div');
    bar.className = 'pagination-bar';
    bar.innerHTML = `
      <div class="pagination-controls">
        <label class="pagination-per-page">
          <span>Per page</span>
          <select class="pagination-select" data-id="${idPrefix}-perpage">
            ${PAGINATION_OPTS.map(n => `<option value="${n}" ${n === perPage ? 'selected' : ''}>${n}</option>`).join('')}
          </select>
        </label>
        <span class="pagination-info">${totalItems ? `${start + 1}–${end} of ${totalItems}` : '0 items'}</span>
        <div class="pagination-nav">
          <button type="button" class="btn btn-secondary btn-pagination" data-action="first" ${curPage <= 0 ? 'disabled' : ''}>«</button>
          <button type="button" class="btn btn-secondary btn-pagination" data-action="prev" ${curPage <= 0 ? 'disabled' : ''}>‹</button>
          <span class="pagination-page">Page ${curPage + 1} of ${totalPages}</span>
          <button type="button" class="btn btn-secondary btn-pagination" data-action="next" ${curPage >= totalPages - 1 ? 'disabled' : ''}>›</button>
          <button type="button" class="btn btn-secondary btn-pagination" data-action="last" ${curPage >= totalPages - 1 ? 'disabled' : ''}>»</button>
        </div>
      </div>
    `;
    bar.querySelector('[data-action="first"]')?.addEventListener('click', () => onPageChange(0));
    bar.querySelector('[data-action="prev"]')?.addEventListener('click', () => onPageChange(Math.max(0, curPage - 1)));
    bar.querySelector('[data-action="next"]')?.addEventListener('click', () => onPageChange(Math.min(totalPages - 1, curPage + 1)));
    bar.querySelector('[data-action="last"]')?.addEventListener('click', () => onPageChange(totalPages - 1));
    bar.querySelector('.pagination-select')?.addEventListener('change', (e) => onPerPageChange(parseInt(e.target.value, 10)));
    return bar;
  }

  const $ = (sel, el = document) => el.querySelector(sel);
  const $$ = (sel, el = document) => el.querySelectorAll(sel);

  const routersUpload = $('#routersUpload');
  const routersTable = $('#routersTable');
  const routersTableSelect = $('#routersTableSelect');
  const routersHead = $('#routersHead');
  const routersHeadSelect = $('#routersHeadSelect');
  const routersBody = $('#routersBody');
  const routersBodySelect = $('#routersBodySelect');
  const routersTableWrap = $('#routersTableWrap');
  const routersTableScroll = $('#routersTableScroll');
  const routersPaginationTop = $('#routersPaginationTop');
  const routersPaginationBottom = $('#routersPaginationBottom');
  const emptyState = $('#emptyState');
  const btnDownload = $('#btnDownload');
  const btnNewRouters = $('#btnNewRouters');
  const btnSave = $('#btnSave');
  const btnSaveAs = $('#btnSaveAs');
  const btnAddRow = $('#btnAddRow');
  const btnAddCol = $('#btnAddCol');
  const drawer = $('#drawer');
  const deploySubtabs = document.querySelectorAll('.deploy-subtab');
  const deployFile = $('#deployFile');
  const btnUploadDeploy = $('#btnUploadDeploy');
  const availableFiles = $('#availableFiles');
  const btnDeploy = $('#btnDeploy');
  const btnDeleteDeployFile = $('#btnDeleteDeployFile');
  const deployStatus = $('#deployStatus');
  const saveAsModal = $('#saveAsModal');
  const saveAsFilename = $('#saveAsFilename');
  const btnSaveAsCancel = $('#btnSaveAsCancel');
  const btnSaveAsConfirm = $('#btnSaveAsConfirm');
  const btnOpen = $('#btnOpen');
  const openModal = $('#openModal');
  const openFileList = $('#openFileList');
  const btnOpenCancel = $('#btnOpenCancel');
  const btnOpenConfirm = $('#btnOpenConfirm');
  const routersToolbar = $('#routersToolbar');
  const btnRouterFileLock = $('#btnRouterFileLock');
  const btnBackupConfig = $('#btnBackupConfig');
  const btnPollRouters = $('#btnPollRouters');
  const pollAuto = $('#pollAuto');
  const pollIntervalMinutes = $('#pollIntervalMinutes');
  const btnDiscoverRouters = $('#btnDiscoverRouters');
  const discoverRoutersModal = $('#discoverRoutersModal');
  const discoverIpRange = $('#discoverIpRange');
  const discoverUsername = $('#discoverUsername');
  const discoverPassword = $('#discoverPassword');
  const discoverPort = $('#discoverPort');
  const btnDiscoverCancel = $('#btnDiscoverCancel');
  const btnDiscoverSubmit = $('#btnDiscoverSubmit');
  const settingsBtn = $('#settingsBtn');
  const settingsModal = $('#settingsModal');
  const helpBtn = $('#helpBtn');
  const helpModal = $('#helpModal');
  const helpClose = $('#helpClose');
  const settingsTimeout = $('#settingsTimeout');
  const settingsRetries = $('#settingsRetries');
  const settingsMaxWorkers = $('#settingsMaxWorkers');
  const settingsMaxWorkersFormula = $('#settingsMaxWorkersFormula');
  const settingsMaxWorkersPerCpu = $('#settingsMaxWorkersPerCpu');
  const settingsUseAsyncClient = $('#settingsUseAsyncClient');
  const btnSettingsCancel = $('#btnSettingsCancel');
  const btnSettingsSave = $('#btnSettingsSave');

  const BASE_COLUMNS = new Set(['state', 'ip_address', 'username', 'password', 'port', 'created_at']);

  function ensureDefaultColumnPaths() {
    Object.keys(COLUMN_DEFAULT_PATHS).forEach(f => {
      if (!state.columnDefaultPaths[f]) {
        state.columnDefaultPaths[f] = COLUMN_DEFAULT_PATHS[f];
      }
    });
  }

  function getVisibleFields() {
    ensureDefaultColumnPaths();
    let cols = state.columns && state.columns.length ? state.columns : DEFAULT_COLUMNS;
    let fields = cols.map(c => normalizeColumnName(c)).filter(k => k);
    fields = fields.filter(f => f !== 'state');
    if (state.routerFileLocked) {
      fields = fields.filter(f => !CREDENTIAL_FIELDS.includes(f));
    }
    const hideWhenLocked = new Set(['state', ...CREDENTIAL_FIELDS]);
    const alwaysHide = new Set(['state']);
    const seen = new Set(fields.map(normalizeColumnName));
    const seenRaw = new Set(fields);
    const extra = [];
    state.routers.forEach(r => {
      Object.keys(r).forEach(k => {
        const norm = normalizeColumnName(k);
        if (!seen.has(norm) && !seenRaw.has(k) && k && !alwaysHide.has(norm) && !(state.routerFileLocked && hideWhenLocked.has(norm))) {
          seen.add(norm);
          seenRaw.add(k);
          extra.push(k);
        }
      });
    });
    return fields.concat(extra);
  }

  function updateRouterFileLockUI() {
    // When no router data, file must be unlocked and lock button disabled
    if (!state.routers.length) {
      state.routerFileLocked = false;
    }
    if (!state.routerFileLocked && state.routersRowExpanded != null) {
      state.routersRowExpanded = null;
      stopRoutersRowExpansionInterval();
    }
    const locked = state.routerFileLocked;
    const hasRouters = state.routers.length > 0;
    if (btnRouterFileLock) {
      btnRouterFileLock.disabled = !hasRouters;
      btnRouterFileLock.classList.toggle('btn-lock-unlocked', !locked);
      btnRouterFileLock.classList.toggle('btn-lock-locked', locked);
      btnRouterFileLock.title = !hasRouters ? 'Load routers to lock' : (locked ? 'LOCKED - click to unlock routers' : 'UNLOCKED - click to lock routers');
      btnRouterFileLock.innerHTML = '';
      const icon = document.createElement('span');
      icon.className = 'lock-icon';
      icon.textContent = locked ? '🔒' : '🔓';
      btnRouterFileLock.appendChild(icon);
      btnRouterFileLock.appendChild(document.createTextNode(' '));
      const fnSpan = document.createElement('span');
      fnSpan.id = 'routersFilename';
      fnSpan.className = 'routers-filename';
      fnSpan.textContent = state.lastFile || '—';
      btnRouterFileLock.appendChild(fnSpan);
    }
    [btnNewRouters, btnOpen, btnDownload, btnSave, btnSaveAs, btnAddRow, btnAddCol, btnDiscoverRouters].forEach(b => {
      if (b) b.disabled = locked;
    });
    if (routersUpload) routersUpload.disabled = locked;
    if (pollAuto) pollAuto.disabled = locked;
    if (pollIntervalMinutes) pollIntervalMinutes.disabled = locked;
    routersToolbar?.classList.toggle('routers-locked', locked);
    routersTableWrap?.classList.toggle('routers-table-locked', locked);
    renderTable();
  }

  function updateRoutersFilenameDisplay() {
    const fnEl = document.getElementById('routersFilename');
    if (fnEl) fnEl.textContent = state.lastFile || '—';
  }

  function getFieldColumnIndex(field) {
    const fields = getVisibleFields();
    return fields.indexOf(field);
  }

  function measureColumnWidth(fieldKey) {
    if (!routersTable) return 100;
    const measurer = document.createElement('span');
    measurer.style.cssText = 'position:absolute;visibility:hidden;white-space:nowrap;pointer-events:none;font:inherit;';
    document.body.appendChild(measurer);
    const displayLabel = state.columnDisplayNames[fieldKey] || prettyPrintFieldName(fieldKey);
    measurer.textContent = displayLabel || ' ';
    let maxW = measurer.offsetWidth;
    const defaultPath = state.columnDefaultPaths[fieldKey];
    if (defaultPath) {
      measurer.textContent = defaultPath;
      maxW = Math.max(maxW, measurer.offsetWidth);
    }
    if (state.routers.length) {
      state.routers.forEach(r => {
        const path = getPathForRouter(r, fieldKey);
        if (path && path !== defaultPath) {
          measurer.textContent = path;
          maxW = Math.max(maxW, measurer.offsetWidth);
        }
        measurer.textContent = String(r[fieldKey] ?? '') || ' ';
        maxW = Math.max(maxW, measurer.offsetWidth);
      });
    }
    document.body.removeChild(measurer);
    const withChrome = maxW + HEADER_CHROME_PX;
    return Math.min(Math.max(withChrome, 100), 450);
  }

  function applyColumnWidths() {
    if (!routersTable || !routersTable.querySelector('colgroup')) return;
    const cols = routersTable.querySelectorAll('colgroup col');
    const fields = getVisibleFields();
    const stateW = state.routersColumnWidths['state'] ?? DEFAULT_COL_WIDTHS.state;
    if (cols[0]) cols[0].style.width = stateW + 'px';
    const PORT_MIN_WIDTH = 90;
    fields.forEach((f, i) => {
      const defaultW = DEFAULT_COL_WIDTHS[f];
      let w = state.routersColumnWidths[f] ?? (defaultW != null ? Math.max(defaultW, measureColumnWidth(f)) : measureColumnWidth(f));
      if (f === 'port') w = Math.max(w, PORT_MIN_WIDTH);
      const colEl = cols[i + 1];
      if (colEl) colEl.style.width = w + 'px';
    });
  }

  function setupColumnResize() {
    if (!routersTable || state.routers.length === 0) return;
    const dataThs = routersTable.querySelectorAll('thead th.routers-sortable');
    let resizeCol = null;
    let startX = 0;
    let startW = 0;

    const onMouseMove = (e) => {
      if (resizeCol == null) return;
      const th = dataThs[resizeCol];
      if (!th) return;
      const dx = e.clientX - startX;
      const newW = Math.max(80, startW + dx);
      const fields = getVisibleFields();
      const fieldName = resizeCol === 0 ? 'state' : fields[resizeCol - 1];
      if (fieldName) state.routersColumnWidths[fieldName] = newW;
      const colgroup = routersTable.querySelector('colgroup');
      if (colgroup) {
          const col = colgroup.querySelectorAll('col')[resizeCol];
          if (col) col.style.width = newW + 'px';
      }
    };
    const onMouseUp = () => {
      resizeCol = null;
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
      saveDrawerUI?.();
    };

    dataThs.forEach((th, colIdx) => {
      let handle = th.querySelector('.routers-col-resize');
      if (!handle) {
        handle = document.createElement('span');
        handle.className = 'routers-col-resize';
        handle.title = 'Drag to resize column';
        th.appendChild(handle);
        handle.addEventListener('mousedown', (e) => {
          e.preventDefault();
          e.stopPropagation();
          resizeCol = colIdx;
          startX = e.clientX;
          const col = routersTable.querySelector(`colgroup col:nth-child(${colIdx + 1})`);
          startW = col ? parseInt(col.style.width, 10) || 100 : 100;
          document.addEventListener('mousemove', onMouseMove);
          document.addEventListener('mouseup', onMouseUp);
          document.body.style.cursor = 'col-resize';
          document.body.style.userSelect = 'none';
        });
      }
    });
  }

  function routersCompareVal(field, rowA, rowB) {
    let a = rowA[field];
    let b = rowB[field];
    if (field === 'state') {
      const va = String(a || '').toLowerCase() === 'online' ? 1 : 0;
      const vb = String(b || '').toLowerCase() === 'online' ? 1 : 0;
      return va < vb ? -1 : va > vb ? 1 : 0;
    }
    if (field === 'port') {
      a = parseInt(a, 10) || 0;
      b = parseInt(b, 10) || 0;
      return a < b ? -1 : a > b ? 1 : 0;
    }
    a = String(a ?? '').trim().toLowerCase();
    b = String(b ?? '').trim().toLowerCase();
    return a < b ? -1 : a > b ? 1 : 0;
  }

  function setRoutersSort(field) {
    const s = state.routersSort;
    if (s.primary === field) {
      s.primaryDir *= -1;
    } else {
      s.secondary = s.primary;
      s.secondaryDir = s.primaryDir;
      s.primary = field;
      s.primaryDir = 1;
    }
    renderTable();
  }

  function renderTable() {
    const savedScroll = (routersTableWrap && routersTableScroll)
      ? { left: routersTableScroll.scrollLeft, top: routersTableWrap.scrollTop }
      : (routersTableWrap ? { left: routersTableWrap.scrollLeft, top: routersTableWrap.scrollTop } : null);
    // When no router data, file must be unlocked and lock button disabled
    if (!state.routers.length) {
      state.routerFileLocked = false;
      if (btnRouterFileLock) btnRouterFileLock.disabled = true;
    } else if (btnRouterFileLock) {
      btnRouterFileLock.disabled = false;
    }
    const fields = getVisibleFields();
    const validSortFields = [...fields, 'state'];
    if (state.routersSort.primary === null || !validSortFields.includes(state.routersSort.primary)) {
      state.routersSort.primary = 'ip_address';
      state.routersSort.secondary = null;
    }

    if (state.routers.length !== (state._routersRowCountPrev ?? -1)) {
      state._routersRowCountPrev = state.routers.length;
      state.routersRowSelected.clear();
    }
    let rows = [...state.routers];
    const p = state.routersSort.primary;
    const s = state.routersSort.secondary;
    if (p) {
      rows.sort((a, b) => {
        const cmp = state.routersSort.primaryDir * routersCompareVal(p, a, b);
        if (cmp !== 0) return cmp;
        if (s) return state.routersSort.secondaryDir * routersCompareVal(s, a, b);
        return 0;
      });
    }

    if (routersTableWrap) routersTableWrap.style.display = 'block';
    emptyState?.classList.remove('visible');
    let savedExpansionHTML = null;
    if (state.routersRowExpanded != null && routersBody) {
      const expandedRow = routersBody.querySelector(`tr.routers-expanded-row[data-state-row="${state.routersRowExpanded}"]`);
      const wrap = expandedRow?.querySelector('.routers-wan-cards-wrap');
      if (wrap?.innerHTML && wrap.querySelector('.routers-wan-sections')) {
        savedExpansionHTML = wrap.innerHTML;
      }
    }
    if (routersHead) routersHead.innerHTML = '';
    if (routersBody) routersBody.innerHTML = '';
    if (routersHeadSelect) routersHeadSelect.innerHTML = '';
    if (routersBodySelect) routersBodySelect.innerHTML = '';
    if (!routersTable || !routersHead || !routersBody) {
      if (savedScroll && routersTableWrap) {
        requestAnimationFrame(() => {
          if (routersTableScroll) routersTableScroll.scrollLeft = savedScroll.left;
          routersTableWrap.scrollTop = savedScroll.top;
        });
      }
      return;
    }

    const useSplit = routersTableSelect && routersHeadSelect && routersBodySelect && routersTableScroll;

    let colgroup = routersTable.querySelector('colgroup');
    if (!colgroup) {
      colgroup = document.createElement('colgroup');
      routersTable.insertBefore(colgroup, routersHead);
    }
    colgroup.innerHTML = '';
    const colState = document.createElement('col');
    colState.classList.add('col-state');
    colState.style.width = '3.75rem';
    colgroup.appendChild(colState);
    fields.forEach((f) => {
      const col = document.createElement('col');
      if (f === 'port') col.classList.add('col-port');
      colgroup.appendChild(col);
    });
    const colActions = document.createElement('col');
    colActions.classList.add('col-actions');
    colActions.style.width = '60px';
    colgroup.appendChild(colActions);

    const headerRow = document.createElement('tr');
    if (useSplit && routersHeadSelect) {
      const selectHeaderRow = document.createElement('tr');
      const thSelectAll = document.createElement('th');
      thSelectAll.className = 'col-select';
      const selectAllCb = document.createElement('input');
      selectAllCb.type = 'checkbox';
      selectAllCb.className = 'routers-select-all';
      selectAllCb.title = 'Select all routers';
      const totalRowCount = state.routers.length;
      selectAllCb.checked = totalRowCount > 0 && totalRowCount === state.routersRowSelected.size;
      selectAllCb.indeterminate = state.routersRowSelected.size > 0 && state.routersRowSelected.size < totalRowCount;
      selectAllCb.onchange = () => {
        if (selectAllCb.checked) state.routers.forEach((_, i) => state.routersRowSelected.add(i));
        else state.routersRowSelected.clear();
        renderTable();
      };
      thSelectAll.appendChild(selectAllCb);
      selectHeaderRow.appendChild(thSelectAll);
      routersHeadSelect.appendChild(selectHeaderRow);
    } else {
      const thSelectAll = document.createElement('th');
      thSelectAll.className = 'col-select';
      const selectAllCb = document.createElement('input');
      selectAllCb.type = 'checkbox';
      selectAllCb.className = 'routers-select-all';
      selectAllCb.title = 'Select all routers';
      const totalRowCount = state.routers.length;
      selectAllCb.checked = totalRowCount > 0 && totalRowCount === state.routersRowSelected.size;
      selectAllCb.indeterminate = state.routersRowSelected.size > 0 && state.routersRowSelected.size < totalRowCount;
      selectAllCb.onchange = () => {
        if (selectAllCb.checked) state.routers.forEach((_, i) => state.routersRowSelected.add(i));
        else state.routersRowSelected.clear();
        renderTable();
      };
      thSelectAll.appendChild(selectAllCb);
      headerRow.appendChild(thSelectAll);
    }
    const sortPrimary = state.routersSort.primary;
    const sortDir = state.routersSort.primaryDir;
    const sortSymbol = sortDir === 1 ? '\u2191' : '\u2193';
    function setSortArrow(th, field) {
      const arrow = th.querySelector('.sort-arrow');
      if (arrow) arrow.textContent = field === sortPrimary ? ' ' + sortSymbol : '';
    }
    const thState = document.createElement('th');
    thState.className = 'col-state routers-sortable';
    thState.dataset.field = 'state';
    thState.title = 'State (Online/Offline)';
    if (state.routerFileLocked) {
      thState.appendChild(document.createTextNode(state.columnDisplayNames.state || 'State'));
    } else {
      const stateNameInput = document.createElement('input');
      stateNameInput.type = 'text';
      stateNameInput.className = 'routers-col-name-input';
      stateNameInput.value = state.columnDisplayNames.state || 'State';
      stateNameInput.title = 'Edit column name';
      stateNameInput.onclick = (e) => e.stopPropagation();
      stateNameInput.onchange = () => {
        const v = stateNameInput.value.trim();
        state.columnDisplayNames.state = v || 'State';
        renderTable();
        saveDrawerUI?.();
      };
      thState.insertBefore(stateNameInput, thState.querySelector('.sort-arrow'));
    }
    const stateArrow = document.createElement('span');
    stateArrow.className = 'sort-arrow';
    stateArrow.setAttribute('aria-hidden', 'true');
    thState.appendChild(stateArrow);
    setSortArrow(thState, 'state');
    headerRow.appendChild(thState);
    const deletableFields = new Set(fields.filter(f => !BASE_COLUMNS.has(f)));
    fields.forEach(f => {
      const th = document.createElement('th');
      th.className = 'routers-sortable';
      if (f === 'port') th.classList.add('col-port');
      if (!state.routerFileLocked) {
        th.classList.add('col-draggable');
        th.draggable = true;
        th.title = (th.title || '') + (th.title ? ' • ' : '') + 'Drag to reorder column';
      }
      th.dataset.field = f;
      if (state.routerFileLocked) {
        th.appendChild(document.createTextNode(state.columnDisplayNames[f] || prettyPrintFieldName(f)));
      } else {
        const nameInput = document.createElement('input');
        nameInput.type = 'text';
        nameInput.className = 'routers-col-name-input';
        nameInput.value = state.columnDisplayNames[f] || prettyPrintFieldName(f);
        nameInput.title = 'Edit column name';
        nameInput.onclick = (e) => e.stopPropagation();
        nameInput.onchange = () => {
          const v = nameInput.value.trim();
          state.columnDisplayNames[f] = v || prettyPrintFieldName(f);
          renderTable();
          saveDrawerUI?.();
        };
        th.appendChild(nameInput);
      }
      const arrow = document.createElement('span');
      arrow.className = 'sort-arrow';
      arrow.setAttribute('aria-hidden', 'true');
      th.appendChild(arrow);
      if (deletableFields.has(f) && !state.routerFileLocked) {
        const delBtn = document.createElement('button');
        delBtn.type = 'button';
        delBtn.className = 'btn-col-delete';
        delBtn.textContent = '×';
        delBtn.title = 'Delete column';
        delBtn.setAttribute('aria-label', 'Delete column');
        delBtn.onclick = (e) => {
          e.stopPropagation();
          deleteColumn(f);
        };
        th.appendChild(delBtn);
      }
      setSortArrow(th, f);
      headerRow.appendChild(th);
    });
    const thActions = document.createElement('th');
    thActions.className = 'col-actions';
    headerRow.appendChild(thActions);
    routersHead.appendChild(headerRow);

    const rowToStateIdx = {};
    rows.forEach((r, i) => {
      const idx = state.routers.indexOf(r);
      rowToStateIdx[i] = idx >= 0 ? idx : i;
    });

    const totalRows = rows.length;
    const perPage = state.routersPerPage;
    const totalPages = Math.max(1, Math.ceil(totalRows / perPage));
    state.routersPage = Math.min(state.routersPage, totalPages - 1);
    const page = Math.max(0, state.routersPage);
    const start = page * perPage;
    const pageRows = rows.slice(start, start + perPage);

    if (state.routersRowExpanded != null) {
      const expandedInPage = pageRows.some((_, j) => rowToStateIdx[start + j] === state.routersRowExpanded);
      if (!expandedInPage) {
        state.routersRowExpanded = null;
        stopRoutersRowExpansionInterval();
      }
    }

    [routersPaginationTop, routersPaginationBottom].forEach(container => {
      if (!container) return;
      container.innerHTML = '';
      if (totalRows > 0) {
        const bar = createPaginationBar('routers', totalRows, page, perPage, (p) => {
          state.routersPage = p;
          renderTable();
        }, (n) => {
          state.routersPerPage = n;
          state.routersPage = 0;
          renderTable();
        });
        container.appendChild(bar);
      }
    });

    pageRows.forEach((row, j) => {
      const rowIdx = start + j;
      const stateRowIdx = rowToStateIdx[rowIdx];
      const tr = document.createElement('tr');
      tr.dataset.stateRow = String(stateRowIdx);
      if (state.routerFileLocked) tr.classList.add('routers-row-clickable');
      if (state.routersRowExpanded === stateRowIdx) tr.classList.add('routers-row-has-expansion');
      const tdSelect = document.createElement('td');
      tdSelect.className = 'col-select';
      const rowCb = document.createElement('input');
      rowCb.type = 'checkbox';
      rowCb.className = 'routers-row-select';
      rowCb.checked = state.routersRowSelected.has(stateRowIdx);
      rowCb.onchange = () => {
        if (rowCb.checked) state.routersRowSelected.add(stateRowIdx);
        else state.routersRowSelected.delete(stateRowIdx);
        const sel = (routersTableSelect || routersTable)?.querySelector('.routers-select-all');
        if (sel) {
          sel.checked = state.routers.length > 0 && state.routers.length === state.routersRowSelected.size;
          sel.indeterminate = state.routersRowSelected.size > 0 && state.routersRowSelected.size < state.routers.length;
        }
        updateRemoteApiButtonState?.();
      };
      tdSelect.appendChild(rowCb);
      if (useSplit && routersBodySelect) {
        const trSelect = document.createElement('tr');
        trSelect.dataset.stateRow = String(stateRowIdx);
        if (state.routersRowExpanded === stateRowIdx) trSelect.classList.add('routers-row-has-expansion');
        trSelect.appendChild(tdSelect);
        routersBodySelect.appendChild(trSelect);
      } else {
        tr.appendChild(tdSelect);
      }
      const st = String(row.state || '').trim().toLowerCase();
      const isOnline = st === 'online' || st === '';  // empty = assume Online until ping updates
      const tdState = document.createElement('td');
      tdState.className = 'col-state router-state-cell';
      const icon = document.createElement('span');
      icon.className = 'router-state-icon ' + (isOnline ? 'state-online' : 'state-offline');
      icon.title = isOnline ? 'Online' : 'Offline';
      icon.setAttribute('aria-label', isOnline ? 'Online' : 'Offline');
      tdState.appendChild(icon);
      tr.appendChild(tdState);
      fields.forEach(f => {
        const td = document.createElement('td');
        if (f === 'port') td.classList.add('col-port');
        const router = state.routers[stateRowIdx];
        const path = getPathForRouter(router, f);
        const wanData = state.routerWanData[stateRowIdx];
        if (path && !PATH_EXCLUDED_FIELDS.has(f)) {
          const val = resolveColumnPath(router, wanData, path, f);
          const wrap = document.createElement('div');
          wrap.className = 'routers-path-cell';
          const span = document.createElement('span');
          span.className = val != null ? 'routers-path-value' : 'routers-path-value routers-path-empty';
          span.textContent = val != null ? val : '';
          span.title = path;
          wrap.appendChild(span);
          if (!state.routerFileLocked) {
            const pathInput = document.createElement('input');
            pathInput.type = 'text';
            pathInput.className = 'routers-path-input';
            pathInput.value = path;
            pathInput.placeholder = 'e.g. status.product_info.product_name, status.wan.devices[0].diagnostics.RSRP';
            pathInput.title = 'Edit API path (per router)';
            pathInput.dataset.field = f;
            pathInput.dataset.row = String(stateRowIdx);
            pathInput.onchange = () => {
              const newPath = pathInput.value.trim();
              if (!router.column_paths) router.column_paths = {};
              if (newPath) router.column_paths[f] = newPath;
              else {
                delete router.column_paths[f];
                if (Object.keys(router.column_paths).length === 0) delete router.column_paths;
              }
              renderTable();
            };
            wrap.appendChild(pathInput);
          }
          td.appendChild(wrap);
        } else if (READONLY_FIELDS.has(f)) {
          const span = document.createElement('span');
          span.className = 'routers-readonly-value';
          span.textContent = row[f] ?? '—';
          td.appendChild(span);
        } else {
          const input = document.createElement('input');
          input.type = f === 'port' ? 'number' : 'text';
          if (f === 'password') input.type = 'password';
          input.value = row[f] ?? '';
          input.dataset.row = stateRowIdx;
          input.dataset.field = f;
          input.onchange = () => {
            state.routers[stateRowIdx][f] = f === 'port' ? (parseInt(input.value, 10) || 8080) : input.value;
          };
          td.appendChild(input);
        }
        tr.appendChild(td);
      });
      const tdActions = document.createElement('td');
      tdActions.className = 'row-actions';
      const delBtn = document.createElement('button');
      delBtn.className = 'btn-icon';
      delBtn.textContent = '×';
      delBtn.title = 'Delete row';
      delBtn.onclick = () => deleteRow(stateRowIdx);
      tdActions.appendChild(delBtn);
      tr.appendChild(tdActions);
      if (state.routerFileLocked) {
        tr.addEventListener('click', (e) => {
          if (e.target.closest('.col-select') || e.target.closest('.row-actions') || e.target.closest('.btn-icon')) return;
          handleRoutersRowExpandClick(stateRowIdx);
        });
      }
      routersBody.appendChild(tr);
      if (state.routersRowExpanded === stateRowIdx) {
        const expTr = document.createElement('tr');
        expTr.className = 'routers-expanded-row';
        expTr.dataset.stateRow = String(stateRowIdx);
        const expTd = document.createElement('td');
        expTd.colSpan = useSplit ? 1 + fields.length + 1 : 2 + fields.length + 1;
        expTd.className = 'routers-expanded-cell';
        const wrap = document.createElement('div');
        wrap.className = 'routers-wan-cards-wrap';
        if (savedExpansionHTML && state.routersRowExpanded === stateRowIdx) {
          wrap.innerHTML = savedExpansionHTML;
        }
        expTd.appendChild(wrap);
        expTr.appendChild(expTd);
        routersBody.appendChild(expTr);
        if (useSplit && routersBodySelect) {
          const expTrSelect = document.createElement('tr');
          expTrSelect.className = 'routers-expanded-row routers-expanded-placeholder';
          expTrSelect.dataset.stateRow = String(stateRowIdx);
          const expTdSelect = document.createElement('td');
          expTdSelect.className = 'col-select routers-expanded-cell-placeholder';
          expTrSelect.appendChild(expTdSelect);
          routersBodySelect.appendChild(expTrSelect);
        }
      }
    });

    if (state.routers.length === 0) {
      routersTableWrap.style.display = 'none';
      emptyState?.classList.add('visible');
    }
    routersTable?.classList.toggle('routers-has-expanded', state.routersRowExpanded != null);
    routersTableSelect?.classList.toggle('routers-has-expanded', state.routersRowExpanded != null);
    applyColumnWidths();
    setupColumnResize();
    updateRemoteApiButtonState?.();
    if (savedScroll && routersTableWrap) {
      requestAnimationFrame(() => {
        if (routersTableScroll) routersTableScroll.scrollLeft = savedScroll.left;
        routersTableWrap.scrollTop = savedScroll.top;
      });
    }
    if (useSplit && routersBody && routersBodySelect) {
      requestAnimationFrame(() => {
        syncSelectTableHeights();
        if (state.routersRowExpanded != null) {
          setTimeout(syncSelectTableHeights, 350);
        }
        if (!state.routerFileLocked) {
          setTimeout(syncSelectTableHeights, 100);
        }
      });
    }
  }

  function syncSelectTableHeights() {
    if (!routersBody || !routersBodySelect) return;
    const dataHeadRow = routersHead?.querySelector('tr');
    const selectHeadRow = routersHeadSelect?.querySelector('tr');
    if (dataHeadRow && selectHeadRow) {
      const th = selectHeadRow.querySelector('th');
      if (th) {
        const h = dataHeadRow.offsetHeight;
        th.style.height = h + 'px';
        th.style.minHeight = h + 'px';
      }
    }
    const dataRows = routersBody.querySelectorAll('tr');
    const selectRows = routersBodySelect.querySelectorAll('tr');
    if (dataRows.length !== selectRows.length) return;
    dataRows.forEach((dataTr, i) => {
      const selectTr = selectRows[i];
      const td = selectTr.querySelector('td');
      if (td) {
        const h = dataTr.offsetHeight;
        td.style.height = h + 'px';
        td.style.minHeight = h + 'px';
      }
    });
  }

  function collectFromTable() {
    if (!routersBody) return state.routers;
    $$('#routersBody tr', document).forEach(tr => {
      const inputs = tr.querySelectorAll('td:not(.row-actions):not(.col-select):not(.col-state) input:not(.routers-path-input)');
      const firstInput = inputs[0];
      const stateRowIdx = firstInput ? parseInt(firstInput.dataset.row ?? '-1', 10) : -1;
      if (stateRowIdx >= 0 && stateRowIdx < state.routers.length) {
        inputs.forEach(input => {
          const f = input.dataset.field;
          if (f) state.routers[stateRowIdx][f] = f === 'port' ? (parseInt(input.value, 10) || 8080) : input.value;
        });
      }
    });
    return state.routers;
  }

  function deleteRow(idx) {
    showConfirmDelete('Delete row?', `Router ${idx + 1} will be permanently removed.`, () => {
      state.routers.splice(idx, 1);
      renderTable();
    });
  }

  function deleteColumn(field) {
    if (BASE_COLUMNS.has(field)) return;
    showConfirmDelete('Delete column?', `Column "${field}" will be removed from all routers.`, () => {
      collectFromTable();
      state.routers.forEach(r => delete r[field]);
      delete state.columnDefaultPaths[field];
      if (state.columns && state.columns.length) {
        state.columns = state.columns.filter(c => normalizeColumnName(c) !== field);
      }
      renderTable();
      if (state.routers.length && state.lastFile) {
        fetch('/api/routers/update', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            routers: state.routers,
            columns: state.columns,
            column_default_paths: state.columnDefaultPaths,
          }),
        }).catch(() => {});
      }
    });
  }

  function reorderColumns(fromIdx, toIdx) {
    if (fromIdx === toIdx || fromIdx < 0 || toIdx < 0) return;
    const fields = getVisibleFields();
    if (fromIdx >= fields.length || toIdx >= fields.length) return;
    const newFields = [...fields];
    const [removed] = newFields.splice(fromIdx, 1);
    newFields.splice(toIdx, 0, removed);
    const stateCol = state.columns?.find(c => normalizeColumnName(c) === 'state') || 'State';
    const seenNorm = new Set();
    const newColumns = [stateCol, ...newFields.map(f => {
      const norm = normalizeColumnName(f);
      if (seenNorm.has(norm)) return null;
      seenNorm.add(norm);
      const dn = state.columns?.find(c => normalizeColumnName(c) === f);
      return dn != null ? dn : (state.columnDisplayNames[f] || prettyPrintFieldName(f));
    }).filter(Boolean)];
    state.columns = newColumns;
    renderTable();
    saveDrawerUI?.();
    if (state.routers.length && state.lastFile) {
      fetch('/api/routers/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          routers: state.routers,
          columns: state.columns,
          column_default_paths: state.columnDefaultPaths,
        }),
      }).catch(() => {});
    }
  }

  function addRow() {
    const r = {};
    ROUTER_BASE_KEYS.forEach(f => {
      if (f === 'port') r[f] = 8080;
      else if (f === 'username') r[f] = 'admin';
      else if (f === 'created_at') r[f] = new Date().toISOString();
      else r[f] = '';
    });
    getColumnFieldKeys().forEach(k => { if (!(k in r)) r[k] = ''; });
    state.routers.push(r);
    renderTable();
  }

  function updatePathColumnCells(stateRowIdx) {
    const pathCols = Object.keys(state.columnDefaultPaths).filter(f => !PATH_EXCLUDED_FIELDS.has(f));
    if (!pathCols.length) return;
    const tr = routersBody?.querySelector(`tr[data-state-row="${stateRowIdx}"]:not(.routers-expanded-row)`);
    if (!tr) return;
    const router = state.routers[stateRowIdx];
    const wanData = state.routerWanData[stateRowIdx];
    const fields = getVisibleFields();
    const tds = tr.querySelectorAll('td:not(.col-select):not(.col-state):not(.row-actions)');
    fields.forEach((f, i) => {
      const path = getPathForRouter(router, f);
        if (path && !PATH_EXCLUDED_FIELDS.has(f) && tds[i]) {
        const span = tds[i].querySelector('.routers-path-value');
        if (span) {
          const val = resolveColumnPath(router, wanData, path, f);
          span.textContent = val != null ? val : '';
          span.classList.toggle('routers-path-empty', val == null);
        }
      }
    });
  }

  const PATH_TO_FLAT_FALLBACK = {
    'config.system.system_id': 'hostname',
    'config.system.desc': 'description',
    'config.system.asset_id': 'asset_id',
    'status.product_info.product_name': 'product_name',
    'status.product_info.mac0': 'mac',
    'status.product_info.mac': 'mac',
    'status.product_info.manufacturing.serial_num': 'serial_number',
    'status.product_info.manufacturing.serial_number': 'serial_number',
    'status.product_info.serial_num': 'serial_number',
    'status.product_info.serial_number': 'serial_number',
    'status.fw_info': 'ncos_version',
  };

  function resolveColumnPath(router, wanData, path, field) {
    if (!path || typeof path !== 'string') return null;
    const pathNorm = toDotBracketPath(path);
    function resolveFirstMdm(obj, remainder) {
      if (!obj || typeof obj !== 'object') return undefined;
      const mdmKey = Object.keys(obj).find(k => k.toLowerCase().startsWith('mdm'));
      if (!mdmKey) return undefined;
      const mdmVal = obj[mdmKey];
      if (remainder) return getValueByPath(mdmVal, remainder);
      return mdmVal;
    }
    const first = (pathNorm.split('.')[0] || '').split('[')[0].toLowerCase();
    let val;
    if (first === 'first_mdm' || first === 'firstmdm') {
      const remainder = pathNorm.replace(/^first_mdm\.?/i, '').replace(/^firstmdm\.?/i, '');
      val = wanData ? resolveFirstMdm(wanData, remainder || null) : undefined;
      if (val == null && router) val = resolveFirstMdm(router, remainder || null);
    } else {
      val = router ? getValueByPath(router, pathNorm) : undefined;
      if (val == null && wanData) val = getValueByPath(wanData, pathNorm);
    }
    if (val == null && router) {
      const fallback = PATH_TO_FLAT_FALLBACK[pathNorm] || PATH_TO_FLAT_FALLBACK[pathNorm.replace(/\.data$/, '')];
      if (fallback && router[fallback] != null) val = router[fallback];
      if (val == null && router[pathNorm] != null) val = router[pathNorm];  // poll stores by path key
      if (val == null && field && router[field] != null) val = router[field];  // poll stores by field key
    }
    if (val == null) return null;
    if (typeof val === 'object') return JSON.stringify(val);
    return String(val);
  }

  function addColumn() {
    const name = prompt('Column name (for custom deploy results):', '');
    if (!name) return;
    const displayName = name.trim() || 'Column';
    const key = normalizeColumnName(displayName) || 'column';
    const pathInput = prompt('Optional API path (e.g. status.product_info.product_name, status.wan.devices[0].diagnostics.RSRP). Leave blank for editable column.', '');
    const path = (pathInput || '').trim();
    if (path) state.columnDefaultPaths[key] = toDotBracketPath(path);
    else delete state.columnDefaultPaths[key];
    if (!state.columns.length) state.columns = [...DEFAULT_COLUMNS];
    if (!state.columns.some(c => normalizeColumnName(c) === key)) {
      state.columns.push(displayName);
    }
    state.routers.forEach(r => { if (!(key in r)) r[key] = ''; });
    renderTable();
  }

  function downloadRouters() {
    if (state.routers.length === 0) {
      showDeployStatus('No data to download.', true);
      return;
    }
    collectFromTable();
    const routersToSave = state.routers.map(normalizeRouterForSave);
    const cols = state.columns && state.columns.length ? state.columns : DEFAULT_COLUMNS;
    const blob = new Blob([JSON.stringify({
      columns: cols,
      column_default_paths: state.columnDefaultPaths,
      routers: routersToSave,
    }, null, 2)], { type: 'application/json' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = state.lastFile || 'routers.json';
    a.click();
    URL.revokeObjectURL(a.href);
  }

  function saveRouters() {
    if (state.routers.length === 0) {
      showDeployStatus('No data to save.', true);
      return;
    }
    collectFromTable();
    let filename = state.lastFile || 'routers.json';
    if (!filename.toLowerCase().endsWith('.json')) filename += '.json';
    const routersToSave = state.routers.map(normalizeRouterForSave);
    const cols = state.columns && state.columns.length ? state.columns : DEFAULT_COLUMNS;
    const data = { columns: cols, column_default_paths: state.columnDefaultPaths, routers: routersToSave, filename };
    function doSave() {
      fetch('/api/routers/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      })
        .then(r => r.json())
        .then(body => {
          if (body.error) showDeployStatus(body.error, true);
          else {
            state.lastFile = filename;
            saveLastFile(filename);
            updateRoutersFilenameDisplay();
            showDeployStatus('Saved to ' + body.saved, false);
          }
        })
        .catch(e => showDeployStatus('Save failed: ' + e.message, true));
    }
    fetch('/api/routers/exists?filename=' + encodeURIComponent(filename))
      .then(r => r.json())
      .then(body => {
        if (body.exists) showConfirmOverwrite(filename, doSave);
        else doSave();
      })
      .catch(() => doSave());
  }

  function saveAsRouters() {
    if (state.routers.length === 0) {
      showDeployStatus('No data to save.', true);
      return;
    }
    saveAsFilename.value = state.lastFile || 'routers.json';
    saveAsModal.classList.add('visible');
  }

  function doSaveAs() {
    let filename = saveAsFilename.value.trim();
    if (!filename) return;
    if (!filename.toLowerCase().endsWith('.json')) filename += '.json';
    collectFromTable();
    const routersToSave = state.routers.map(normalizeRouterForSave);
    const cols = state.columns && state.columns.length ? state.columns : DEFAULT_COLUMNS;
    const data = { columns: cols, column_default_paths: state.columnDefaultPaths, routers: routersToSave, filename };
    function performSaveAs() {
      fetch('/api/routers/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      })
        .then(r => r.json())
        .then(body => {
          saveAsModal.classList.remove('visible');
          if (body.error) showDeployStatus(body.error, true);
          else {
            state.lastFile = filename;
            saveLastFile(filename);
            updateRoutersFilenameDisplay();
            showDeployStatus('Saved to ' + body.saved, false);
          }
        })
        .catch(e => {
          saveAsModal.classList.remove('visible');
          showDeployStatus('Save failed: ' + e.message, true);
        });
    }
    fetch('/api/routers/exists?filename=' + encodeURIComponent(filename))
      .then(r => r.json())
      .then(body => {
        if (body.exists) {
          saveAsModal.classList.remove('visible');
          showConfirmOverwrite(filename, performSaveAs);
        } else {
          performSaveAs();
        }
      })
      .catch(() => performSaveAs());
  }

  function uploadRouters(ev) {
    const file = ev.target.files?.[0];
    if (!file) return;
    const fd = new FormData();
    fd.append('file', file);
    fetch('/api/routers/upload', { method: 'POST', body: fd })
      .then(r => r.json())
      .then(body => {
        if (body.error) {
          showDeployStatus(body.error, true);
          return;
        }
        state.routers = (body.routers || []).map(sanitizeRouterColumnPaths);
        state.columns = body.columns && body.columns.length ? body.columns : [];
        state.columnDefaultPaths = body.column_default_paths && typeof body.column_default_paths === 'object' ? { ...body.column_default_paths } : {};
        state.lastFile = file.name || '';
        updateRoutersFilenameDisplay();
        renderTable();
        showDeployStatus('Loaded: ' + state.routers.length + ' routers', false);
      })
      .catch(e => showDeployStatus('Upload failed: ' + e.message, true));
    ev.target.value = '';
  }

  // Drawer tabs
  const drawerTabDeployment = $('#drawerTabDeployment');
  const drawerTabMonitoring = $('#drawerTabMonitoring');
  const drawerTabLogs = $('#drawerTabLogs');
  const drawerPaneDeployment = $('#drawerPaneDeployment');
  const drawerPaneMonitoring = $('#drawerPaneMonitoring');
  const drawerPaneLogs = $('#drawerPaneLogs');
  const drawerArrowDeployment = $('#drawerArrowDeployment');
  const drawerArrowMonitoring = $('#drawerArrowMonitoring');
  const drawerArrowLogs = $('#drawerArrowLogs');

  function updateDrawerArrows() {
    const expanded = drawer.classList.contains('expanded');
    const arrow = expanded ? '\u25B6' : '\u25C0'; /* right when open, left when closed */
    if (drawerArrowDeployment) drawerArrowDeployment.textContent = arrow;
    if (drawerArrowMonitoring) drawerArrowMonitoring.textContent = arrow;
    if (drawerArrowLogs) drawerArrowLogs.textContent = arrow;
  }

  function setDrawerTab(tabName) {
    drawer.classList.add('expanded');
    updateDrawerArrows();
    const tabs = [
      { tab: drawerTabDeployment, pane: drawerPaneDeployment, name: 'deployment' },
      { tab: drawerTabMonitoring, pane: drawerPaneMonitoring, name: 'monitoring' },
      { tab: drawerTabLogs, pane: drawerPaneLogs, name: 'logs' },
    ];
    tabs.forEach(({ tab, pane, name }) => {
      if (name === tabName) {
        tab?.classList.add('active');
        pane?.classList.add('active');
      } else {
        tab?.classList.remove('active');
        pane?.classList.remove('active');
      }
    });
    if (tabName === 'logs') loadLogFileList();
    saveDrawerUI?.();
  }

  function setupDrawerTabClick(el, tabName) {
    el?.addEventListener('click', () => {
      if (el.classList.contains('active') && drawer.classList.contains('expanded')) {
        drawer.classList.remove('expanded');
        updateDrawerArrows();
        saveDrawerUI?.();
      } else {
        setDrawerTab(tabName);
      }
    });
  }
  setupDrawerTabClick(drawerTabDeployment, 'deployment');
  setupDrawerTabClick(drawerTabMonitoring, 'monitoring');
  setupDrawerTabClick(drawerTabLogs, 'logs');
  let drawerRestored = false;
  try {
    const uiRaw = localStorage.getItem('prm_drawerUI');
    if (uiRaw) {
      const ui = JSON.parse(uiRaw);
      const tab = ui.drawerTab || 'deployment';
      const tabs = [
        { tab: drawerTabDeployment, pane: drawerPaneDeployment, name: 'deployment' },
        { tab: drawerTabMonitoring, pane: drawerPaneMonitoring, name: 'monitoring' },
        { tab: drawerTabLogs, pane: drawerPaneLogs, name: 'logs' },
      ];
      tabs.forEach(({ tab: t, pane: p, name }) => {
        if (name === tab) {
          t?.classList.add('active');
          p?.classList.add('active');
        } else {
          t?.classList.remove('active');
          p?.classList.remove('active');
        }
      });
      if (ui.drawerExpanded === false) {
        drawer.classList.remove('expanded');
      } else {
        drawer.classList.add('expanded');
      }
      updateDrawerArrows();
      if (tab === 'logs') loadLogFileList?.();
      drawerRestored = true;
    }
  } catch (_) {}
  if (!drawerRestored) {
    drawer.classList.add('expanded');
    setDrawerTab('deployment');
  }

  // Drawer resize
  const drawerResizeHandle = $('#drawerResizeHandle');
  const DRAWER_MIN = 480;
  const DRAWER_MAX = 900;
  function getDefaultDrawerWidth() {
    return Math.min(DRAWER_MAX, Math.max(DRAWER_MIN, Math.floor(window.innerWidth * 0.45)));
  }
  function applyDrawerWidth(w) {
    const px = w + 'px';
    document.documentElement.style.setProperty('--drawer-width', px);
    try { localStorage.setItem('drawerWidth', String(w)); } catch (_) {}
  }
  function initDrawerWidth() {
    const saved = localStorage.getItem('drawerWidth');
    const w = saved ? parseInt(saved, 10) : getDefaultDrawerWidth();
    applyDrawerWidth(Math.min(DRAWER_MAX, Math.max(DRAWER_MIN, w)));
  }
  initDrawerWidth();
  drawerResizeHandle?.addEventListener('mousedown', (e) => {
    e.preventDefault();
    const startX = e.clientX;
    const startW = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--drawer-width')) || 640;
    function onMove(ev) {
      const delta = startX - ev.clientX;
      const newW = Math.min(DRAWER_MAX, Math.max(DRAWER_MIN, startW + delta));
      applyDrawerWidth(newW);
    }
    function onUp() {
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
    }
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
  });

  // Logs tab
  const logFileSelect = $('#logFileSelect');
  const logContentViewer = $('#logContentViewer');
  const btnLogDownload = $('#btnLogDownload');
  const btnLogRename = $('#btnLogRename');
  const btnLogDelete = $('#btnLogDelete');

  function loadLogFileList() {
    if (!logFileSelect) return;
    fetch('/api/logs/list')
      .then(r => r.json())
      .then(data => {
        const files = data.files || [];
        logFileSelect.innerHTML = files.map(f => `<option value="${escapeHtml(f.name)}">${escapeHtml(f.name)}</option>`).join('');
        logContentViewer && (logContentViewer.textContent = '');
        btnLogDownload && (btnLogDownload.disabled = true);
        btnLogRename && (btnLogRename.disabled = true);
        btnLogDelete && (btnLogDelete.disabled = true);
        if (files.length) {
          const sel = logFileSelect.options[0];
          if (sel) loadLogContent(sel.value);
        }
      })
      .catch(() => {});
  }

  function loadLogContent(filename) {
    if (!filename || !logContentViewer) return;
    logContentViewer.textContent = 'Loading...';
    fetch('/api/logs/read?filename=' + encodeURIComponent(filename))
      .then(r => r.json())
      .then(data => {
        logContentViewer.textContent = data.content ?? '';
        if (btnLogDownload) {
          btnLogDownload.disabled = false;
          btnLogDownload.dataset.filename = filename;
        }
        if (btnLogRename) {
          btnLogRename.disabled = false;
          btnLogRename.dataset.filename = filename;
        }
        if (btnLogDelete) {
          btnLogDelete.disabled = false;
          btnLogDelete.dataset.filename = filename;
        }
      })
      .catch(() => { logContentViewer.textContent = 'Failed to load.'; });
  }

  logFileSelect?.addEventListener('change', () => {
    const sel = logFileSelect.options[logFileSelect.selectedIndex];
    if (sel) loadLogContent(sel.value);
  });

  btnLogDownload?.addEventListener('click', () => {
    const fn = btnLogDownload.dataset.filename;
    if (fn) window.location.href = '/api/logs/download/' + encodeURIComponent(fn);
  });

  btnLogRename?.addEventListener('click', () => {
    const fn = btnLogRename.dataset.filename;
    if (!fn) return;
    const newName = prompt('Enter new filename:', fn);
    if (!newName || newName.trim() === fn.trim()) return;
    const trimmed = newName.trim();
    if (!trimmed.toLowerCase().endsWith('.log') && !trimmed.toLowerCase().endsWith('.csv')) {
      showDeployStatus?.('Filename must end with .log or .csv', true);
      return;
    }
    fetch('/api/logs/rename', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filename: fn, new_filename: trimmed }),
    })
      .then(r => r.json())
      .then(body => {
        if (body.error) showDeployStatus?.(body.error, true);
        else {
          loadLogFileList();
          showDeployStatus?.('File renamed to ' + trimmed, false);
        }
      })
      .catch(() => showDeployStatus?.('Rename failed.', true));
  });

  const confirmDeleteModal = $('#confirmDeleteModal');
  const confirmDeleteTitle = $('#confirmDeleteTitle');
  const confirmDeleteMessage = $('#confirmDeleteMessage');
  const confirmDeleteHint = $('#confirmDeleteHint');
  const btnConfirmDeleteCancel = $('#btnConfirmDeleteCancel');
  const btnConfirmDeleteOk = $('#btnConfirmDeleteOk');

  let confirmDeleteCallback = null;

  function showConfirmDelete(title, message, onConfirm, hint = 'This cannot be undone.', confirmLabel = 'Delete') {
    confirmDeleteTitle.textContent = title;
    confirmDeleteMessage.textContent = message;
    confirmDeleteHint.textContent = hint;
    confirmDeleteHint.style.display = hint ? 'block' : 'none';
    if (btnConfirmDeleteOk) btnConfirmDeleteOk.textContent = confirmLabel;
    btnConfirmDeleteOk?.classList.toggle('btn-danger', confirmLabel === 'Delete');
    btnConfirmDeleteOk?.classList.toggle('btn-primary', confirmLabel !== 'Delete');
    confirmDeleteCallback = onConfirm;
    confirmDeleteModal.classList.add('visible');
  }

  function showConfirmOverwrite(filename, onConfirm) {
    showConfirmDelete('Overwrite file?', filename + ' already exists.', onConfirm, '', 'Overwrite');
  }

  function closeConfirmDeleteModal() {
    confirmDeleteModal?.classList.remove('visible');
    confirmDeleteCallback = null;
  }

  function runConfirmDelete() {
    if (confirmDeleteCallback) {
      const fn = confirmDeleteCallback;
      confirmDeleteCallback = null;
      closeConfirmDeleteModal();
      fn();
    }
  }

  btnConfirmDeleteCancel?.addEventListener('click', closeConfirmDeleteModal);
  btnConfirmDeleteOk?.addEventListener('click', () => {
    if (confirmDeleteCallback) runConfirmDelete();
  });

  confirmDeleteModal?.addEventListener('click', (e) => {
    if (e.target === confirmDeleteModal) closeConfirmDeleteModal();
  });

  btnLogDelete?.addEventListener('click', () => {
    const fn = btnLogDelete.dataset.filename;
    if (!fn) return;
    showConfirmDelete('Delete log file?', fn, () => {
      fetch('/api/logs/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filename: fn }),
      })
        .then(r => r.json())
        .then(body => {
          if (body.error) showDeployStatus?.(body.error, true);
          else loadLogFileList();
        })
        .catch(() => showDeployStatus?.('Delete failed.', true));
    });
  });

  // Theme toggle
  const themeToggle = $('#themeToggle');
  function initTheme() {
    const saved = localStorage.getItem('theme');
    if (saved === 'light') {
      document.body.classList.add('light-mode');
      if (themeToggle) themeToggle.textContent = '🌙';
    } else {
      document.body.classList.remove('light-mode');
      if (themeToggle) themeToggle.textContent = '☀';
    }
  }
  if (themeToggle) {
    themeToggle.addEventListener('click', () => {
      document.body.classList.toggle('light-mode');
      const isLight = document.body.classList.contains('light-mode');
      localStorage.setItem('theme', isLight ? 'light' : 'dark');
      themeToggle.textContent = isLight ? '🌙' : '☀';
    });
  }
  initTheme();

  const sshPortField = $('#sshPortField');
  const sshPortInput = $('#sshPort');

  const ncosDownloadSection = $('#ncosDownloadSection');
  const ncosDownloadToggle = $('#ncosDownloadToggle');
  const ncosDownloadContent = $('#ncosDownloadContent');
  const ncosApiStatus = $('#ncosApiStatus');
  const ncosCpId = $('#ncosCpId');
  const ncosCpKey = $('#ncosCpKey');
  const ncosEcmId = $('#ncosEcmId');
  const ncosEcmKey = $('#ncosEcmKey');
  const btnSaveNcosKeys = $('#btnSaveNcosKeys');
  const ncosVersion = $('#ncosVersion');
  const ncosModel = $('#ncosModel');
  const btnNcosSearch = $('#btnNcosSearch');
  const ncosFirmwareList = $('#ncosFirmwareList');
  const btnNcosDownload = $('#btnNcosDownload');

  if (ncosDownloadToggle) {
    ncosDownloadToggle.addEventListener('click', () => {
      ncosDownloadSection.classList.toggle('expanded');
      ncosDownloadToggle.setAttribute('aria-expanded', ncosDownloadSection.classList.contains('expanded'));
    });
  }

  const fileUploadSection = $('#fileUploadSection');
  const availableFilesSection = $('#availableFilesSection');

  function setDeployType(type) {
    state.deployType = type;
    deploySubtabs.forEach(btn => {
      const active = btn.dataset.type === type;
      btn.classList.toggle('active', active);
    });
    fileUploadSection.style.display = 'block';
    availableFilesSection.style.display = 'block';
    if (btnDeploy) btnDeploy.style.display = 'block';
    if (btnDeleteDeployFile) btnDeleteDeployFile.style.display = 'block';
    sshPortField.style.display = state.deployType === 'sdk_apps' ? 'block' : 'none';
    ncosDownloadSection.style.display = state.deployType === 'ncos' ? 'block' : 'none';
    if (state.deployType === 'ncos') {
      loadNcosConfig();
      ncosDownloadSection.classList.remove('expanded');
    }
    loadAvailableFiles();
  }
  deploySubtabs.forEach(btn => {
    btn.addEventListener('click', () => setDeployType(btn.dataset.type));
  });

  function loadNcosConfig() {
    fetch('/api/ncos/config')
      .then(r => r.json())
      .then(cfg => {
        if (ncosApiStatus) {
          if (cfg.configured && cfg.source === 'env') {
            ncosApiStatus.textContent = 'API keys loaded from environment';
            ncosApiStatus.className = 'ncos-api-status ncos-status-ok';
            ncosCpId.value = cfg['X-CP-API-ID'] || '';
            ncosCpKey.value = cfg['X-CP-API-KEY'] || '';
            ncosEcmId.value = cfg['X-ECM-API-ID'] || '';
            ncosEcmKey.value = cfg['X-ECM-API-KEY'] || '';
            if (btnSaveNcosKeys) btnSaveNcosKeys.disabled = true;
          } else if (cfg.configured) {
            ncosApiStatus.textContent = '';
            ncosApiStatus.className = 'ncos-api-status';
            ncosCpId.value = cfg['X-CP-API-ID'] || '';
            ncosCpKey.value = cfg['X-CP-API-KEY'] || '';
            ncosEcmId.value = cfg['X-ECM-API-ID'] || '';
            ncosEcmKey.value = cfg['X-ECM-API-KEY'] || '';
            if (btnSaveNcosKeys) btnSaveNcosKeys.disabled = false;
          } else {
            ncosApiStatus.textContent = '';
            ncosApiStatus.className = 'ncos-api-status';
            if (btnSaveNcosKeys) btnSaveNcosKeys.disabled = false;
          }
        }
      })
      .catch(() => {});
  }

  if (btnSaveNcosKeys) {
    btnSaveNcosKeys.addEventListener('click', () => {
      fetch('/api/ncos/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          'X-CP-API-ID': ncosCpId.value,
          'X-CP-API-KEY': ncosCpKey.value,
          'X-ECM-API-ID': ncosEcmId.value,
          'X-ECM-API-KEY': ncosEcmKey.value,
        }),
      })
        .then(r => r.json())
        .then(() => showDeployStatus('API keys saved.', false))
        .catch(e => showDeployStatus('Save failed: ' + e.message, true));
    });
  }

  if (btnNcosSearch) {
    btnNcosSearch.addEventListener('click', () => {
      const version = ncosVersion.value.trim();
      const model = ncosModel.value.trim();
      if (!version || !model) {
        showDeployStatus('Enter version and model.', true);
        return;
      }
      showDeployStatus('Searching...', false);
      fetch('/api/ncos/firmwares?version=' + encodeURIComponent(version) + '&model=' + encodeURIComponent(model))
        .then(r => r.json())
        .then(body => {
          if (body.error) {
            showDeployStatus(body.error, true);
            return;
          }
          ncosFirmwareList.innerHTML = '';
          (body.firmwares || []).forEach((f, i) => {
            const opt = document.createElement('option');
            opt.value = f.url;
            opt.textContent = (f.url || '').replace(/\//g, '');
            opt.dataset.url = f.url;
            ncosFirmwareList.appendChild(opt);
          });
          showDeployStatus((body.firmwares || []).length + ' NCOS file(s) found.', false);
        })
        .catch(e => showDeployStatus('Search failed: ' + e.message, true));
    });
  }

  if (btnNcosDownload) {
    btnNcosDownload.addEventListener('click', () => {
      const version = ncosVersion.value.trim();
      const model = ncosModel.value.trim();
      const opt = ncosFirmwareList.selectedOptions[0];
      const url = opt ? opt.value : '';
      if (!version || !model || !url) {
        showDeployStatus('Search first and select an NCOS.', true);
        return;
      }
      showDeployStatus('Downloading...', false);
      fetch('/api/ncos/download', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version, model, url }),
      })
        .then(r => r.json())
        .then(body => {
          if (body.error) showDeployStatus(body.error, true);
          else {
            loadAvailableFiles();
            showDeployStatus('Downloaded: ' + body.name, false);
          }
        })
        .catch(e => showDeployStatus('Download failed: ' + e.message, true));
    });
  }

  function loadAvailableFiles() {
    fetch('/api/files/' + state.deployType)
      .then(r => r.json())
      .then(body => {
        availableFiles.innerHTML = '';
        (body.files || []).forEach(f => {
          const opt = document.createElement('option');
          opt.value = f.path;
          opt.textContent = f.name;
          availableFiles.appendChild(opt);
        });
      })
      .catch(() => {});
  }

  if (btnDiscoverRouters) {
    btnDiscoverRouters.addEventListener('click', () => {
      discoverIpRange.value = '';
      if (!discoverUsername?.value?.trim()) discoverUsername.value = 'admin';
      if (!discoverPort?.value?.trim()) discoverPort.value = '8080';
      discoverRoutersModal?.classList.add('visible');
    });
  }
  btnDiscoverCancel?.addEventListener('click', () => discoverRoutersModal?.classList.remove('visible'));
  btnDiscoverSubmit?.addEventListener('click', () => {
    const ipRange = discoverIpRange?.value?.trim();
    const username = discoverUsername?.value?.trim() || 'admin';
    const password = discoverPassword?.value?.trim();
    const port = parseInt(discoverPort?.value, 10) || 8080;
    if (!ipRange) {
      showDeployStatus('Enter IP subnet or range.', true);
      return;
    }
    if (!password) {
      showDeployStatus('Password required.', true);
      return;
    }
    discoverRoutersModal?.classList.remove('visible');
    showDeployStatus('Discovering routers...', false);
    fetch('/api/discover-routers', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ip_range: ipRange, username, password, port }),
    })
      .then(r => r.json().then(body => ({ ok: r.ok, status: r.status, body })))
      .then(({ ok, body }) => {
        if (body.log_file) loadLogFileList?.();
        if (!ok || body.error) {
          const msg = body.error
            ? (body.log_file ? `${body.error} Log saved to ${body.log_file}.` : body.error)
            : 'Discover failed.';
          showDeployStatus(msg, true);
        } else {
          state.routers = (body.routers || []).map(sanitizeRouterColumnPaths);
          if (body.last_file) state.lastFile = body.last_file;
          if (body.columns && body.columns.length) state.columns = body.columns;
          if (body.column_default_paths && typeof body.column_default_paths === 'object') state.columnDefaultPaths = { ...body.column_default_paths };
          renderTable();
          updateRoutersFilenameDisplay();
          const status = body.log_file ? `Discover complete. Log: ${body.log_file}.` : 'Discover complete.';
          showDeployStatus(status, false);
        }
      })
      .catch(e => showDeployStatus('Discover failed: ' + e.message, true));
  });

  function pollRoutersWithStream(routers, onProgress, onComplete) {
    const useStream = routers.length > 50;
    const body = JSON.stringify({
      routers,
      stream: useStream,
      column_default_paths: state.columnDefaultPaths,
    });
    return fetch('/api/get-router-info', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: body,
    }).then(response => {
      const ct = response.headers.get('content-type') || '';
      if (useStream && ct.includes('ndjson')) {
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buf = '';
        function read() {
          return reader.read().then(({ value, done }) => {
            if (value) buf += decoder.decode(value, { stream: true });
            const lines = buf.split('\n');
            buf = lines.pop() || '';
            for (const line of lines) {
              if (!line.trim()) continue;
              try {
                const ev = JSON.parse(line);
                if (ev.event === 'router' && onProgress) onProgress(ev.ip, ev.router);
                if (ev.event === 'complete' && onComplete) onComplete(ev.routers);
              } catch (_) {}
            }
            if (!done) return read();
          });
        }
        return read();
      }
      return response.json().then(body => {
        if (body.error) throw new Error(body.error);
        if (onComplete && body.routers) onComplete(body.routers);
      });
    });
  }

  function runPollRouters() {
    if (!state.routers.length) {
      showDeployStatus?.('Load routers file first.', true);
      return;
    }
    const routers = state.routers;
    showDeployStatus?.('Polling routers...', false);
    pollRoutersWithStream(routers,
      (ip, router) => {
        const idx = state.routers.findIndex(r => (r.ip_address || '').split(':')[0] === ip);
        if (idx >= 0) {
          const existing = state.routers[idx];
          const preserved = existing?.column_paths && typeof existing.column_paths === 'object' ? existing.column_paths : undefined;
          state.routers[idx] = router;
          if (preserved) state.routers[idx].column_paths = preserved;
        }
        renderTable();
      },
      (routersResult) => {
        if (routersResult) {
          const columnPathsByIp = {};
          state.routers.forEach(r => {
            const ip = (r.ip_address || '').split(':')[0];
            if (ip && r.column_paths && typeof r.column_paths === 'object') columnPathsByIp[ip] = r.column_paths;
          });
          state.routers = routersResult.map(sanitizeRouterColumnPaths);
          state.routers.forEach(server => {
            const ip = (server.ip_address || '').split(':')[0];
            if (ip && columnPathsByIp[ip]) server.column_paths = columnPathsByIp[ip];
          });
        }
        renderTable();
        showDeployStatus?.('Poll complete.', false);
      }
    ).catch(e => showDeployStatus?.('Poll failed: ' + e.message, true));
  }

  let pollAutoTimer = null;

  if (btnPollRouters) {
    btnPollRouters.addEventListener('click', () => {
      if (!state.routers.length) {
        showDeployStatus('Load routers file first.', true);
        return;
      }
      runPollRouters();
    });
  }

  pollAuto?.addEventListener('change', () => {
    clearInterval(pollAutoTimer);
    pollAutoTimer = null;
    if (pollAuto.checked) {
      runPollRouters();
      const min = parseInt(pollIntervalMinutes?.value, 10) || 60;
      pollAutoTimer = setInterval(runPollRouters, Math.max(1, min) * 60 * 1000);
    }
    saveDrawerUI?.();
  });

  pollIntervalMinutes?.addEventListener('change', () => {
    if (pollAuto?.checked && pollAutoTimer) {
      clearInterval(pollAutoTimer);
      const min = parseInt(pollIntervalMinutes?.value, 10) || 60;
      pollAutoTimer = setInterval(runPollRouters, Math.max(1, min) * 60 * 1000);
    }
    saveDrawerUI?.();
  });

  if (settingsBtn) {
    settingsBtn.addEventListener('click', () => {
      fetch('/api/config/app')
        .then(r => r.json())
        .then(cfg => {
          settingsTimeout.value = cfg.connection_timeout ?? state.connectionTimeout;
          settingsRetries.value = cfg.connection_retries ?? state.connectionRetries;
          settingsMaxWorkers.value = cfg.max_workers ?? state.maxWorkers;
          settingsMaxWorkersFormula.value = cfg.max_workers_formula ?? state.maxWorkersFormula;
          settingsMaxWorkersPerCpu.value = cfg.max_workers_per_cpu ?? state.maxWorkersPerCpu;
          settingsUseAsyncClient.checked = cfg.use_async_client ?? state.useAsyncClient;
          settingsModal?.classList.add('visible');
        })
        .catch(() => {
          settingsTimeout.value = state.connectionTimeout;
          settingsRetries.value = state.connectionRetries;
          settingsMaxWorkers.value = state.maxWorkers;
          settingsMaxWorkersFormula.value = state.maxWorkersFormula;
          settingsMaxWorkersPerCpu.value = state.maxWorkersPerCpu;
          settingsUseAsyncClient.checked = state.useAsyncClient;
          settingsModal?.classList.add('visible');
        });
    });
  }
  btnSettingsCancel?.addEventListener('click', () => settingsModal?.classList.remove('visible'));
  if (btnSettingsSave) {
    btnSettingsSave.addEventListener('click', () => {
      const timeout = parseInt(settingsTimeout?.value, 10) || 2;
      const retries = parseInt(settingsRetries?.value, 10) || 1;
      const maxWorkers = parseInt(settingsMaxWorkers?.value, 10) || 64;
      const maxWorkersFormula = settingsMaxWorkersFormula?.value || 'sqrt';
      const maxWorkersPerCpu = parseInt(settingsMaxWorkersPerCpu?.value, 10) || 4;
      const useAsyncClient = settingsUseAsyncClient?.checked ?? false;
      settingsModal?.classList.remove('visible');
      fetch('/api/config/app', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          connection_timeout: timeout,
          connection_retries: retries,
          max_workers: maxWorkers,
          max_workers_formula: maxWorkersFormula,
          max_workers_per_cpu: maxWorkersPerCpu,
          use_async_client: useAsyncClient,
        }),
      })
        .then(r => r.json())
        .then(() => {
          state.connectionTimeout = timeout;
          state.connectionRetries = retries;
          state.maxWorkers = maxWorkers;
          state.maxWorkersFormula = maxWorkersFormula;
          state.maxWorkersPerCpu = maxWorkersPerCpu;
          state.useAsyncClient = useAsyncClient;
          showDeployStatus('Settings saved.', false);
        })
        .catch(() => showDeployStatus('Failed to save settings.', true));
    });
  }

  btnUploadDeploy.addEventListener('click', () => deployFile.click());

  deployFile.addEventListener('change', ev => {
    const file = ev.target.files?.[0];
    if (!file) return;
    const fd = new FormData();
    fd.append('file', file);
    fetch('/api/files/' + state.deployType + '/upload', { method: 'POST', body: fd })
      .then(r => r.json())
      .then(body => {
        if (body.error) showDeployStatus(body.error, true);
        else {
          loadAvailableFiles();
          showDeployStatus('Uploaded: ' + body.name, false);
        }
      })
      .catch(e => showDeployStatus('Upload failed: ' + e.message, true));
    ev.target.value = '';
  });

  function showDeployStatus(msg, isError) {
    deployStatus.textContent = msg;
    deployStatus.className = 'status-bar' + (isError ? ' error' : ' success');
  }

  function setDeployButtonLoading(loading) {
    if (!btnDeploy) return;
    btnDeploy.disabled = loading;
    if (loading) {
      btnDeploy.innerHTML = '<span class="btn-spinner"></span> Deploying...';
    } else {
      btnDeploy.textContent = 'Deploy';
    }
  }

  btnDeploy.addEventListener('click', () => {
    const path = availableFiles.value;
    if (!path) {
      showDeployStatus('Select a file to deploy.', true);
      return;
    }
    collectFromTable();
    if (!state.routers.length) {
      showDeployStatus('Load routers file first.', true);
      return;
    }
    const selected = [...state.routersRowSelected].filter(i => i >= 0 && i < state.routers.length);
    if (!selected.length) {
      showDeployStatus('Select one or more routers.', true);
      return;
    }
    const allSelected = selected.length === state.routers.length;
    const deployTypeLabel = { licenses: 'License', ncos: 'NCOS', configuration: 'Configuration', sdk_apps: 'SDK App' }[state.deployType] || state.deployType;
    const doDeploy = () => {
      const data = {
        file_path: path,
        deploy_type: state.deployType,
        ssh_port: parseInt(sshPortInput.value, 10) || 22,
        indices: selected,
      };
      setDeployButtonLoading(true);
      fetch('/api/routers/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ routers: state.routers }),
      })
        .then(() =>
          fetch('/api/deploy', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data),
          })
        )
        .then(r => r.json())
        .then(body => {
          if (body.error) showDeployStatus(body.error, true);
          else {
            if (body.routers) state.routers = body.routers.map(sanitizeRouterColumnPaths);
            renderTable();
            if (state.deployType === 'sdk_apps' && body.log_file) {
              const n = body.failure_count ?? 0;
              showDeployStatus(`Deployment complete: ${n} failure${n !== 1 ? 's' : ''}. Log saved to ${body.log_file}`, n > 0);
              loadLogFileList?.();
            } else {
              showDeployStatus('Deployment complete. Results saved to CSV.', false);
            }
          }
        })
        .catch(e => showDeployStatus('Deploy failed: ' + e.message, true))
        .finally(() => setDeployButtonLoading(false));
    };
    if (allSelected) {
      showConfirmDelete?.('Deploy to all routers?', `${deployTypeLabel} will be deployed to all ${selected.length} router(s).`, doDeploy, '', 'Deploy');
    } else {
      doDeploy();
    }
  });

  btnDeleteDeployFile?.addEventListener('click', () => {
    const path = availableFiles?.value;
    if (!path) {
      showDeployStatus?.('Select a file to delete.', true);
      return;
    }
    const filename = path.replace(/^.*[/\\]/, '');
    const folderNames = { licenses: 'licenses', ncos: 'NCOS', configuration: 'configs', sdk_apps: 'sdk_apps' };
    const folder = folderNames[state.deployType] || 'folder';
    showConfirmDelete?.('Delete file?', `"${filename}" will be permanently removed from the ${folder} folder.`, () => {
      fetch('/api/files/' + state.deployType + '/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filename }),
      })
        .then(r => r.json())
        .then(body => {
          if (body.error) showDeployStatus?.(body.error, true);
          else {
            loadAvailableFiles();
            showDeployStatus?.('File deleted.', false);
          }
        })
        .catch(e => showDeployStatus?.('Delete failed: ' + e.message, true));
    });
  });

  function loadAppConfig() {
    fetch('/api/config/app')
      .then(r => r.json())
      .then(cfg => {
        if (cfg.connection_timeout != null) state.connectionTimeout = cfg.connection_timeout;
        if (cfg.connection_retries != null) state.connectionRetries = cfg.connection_retries;
        if (cfg.max_workers != null) state.maxWorkers = cfg.max_workers;
        if (cfg.max_workers_formula != null) state.maxWorkersFormula = cfg.max_workers_formula;
        if (cfg.max_workers_per_cpu != null) state.maxWorkersPerCpu = cfg.max_workers_per_cpu;
        if (cfg.use_async_client != null) state.useAsyncClient = cfg.use_async_client;
        if (cfg.last_file) {
          state.lastFile = cfg.last_file;
          fetch('/api/routers/open?filename=' + encodeURIComponent(cfg.last_file))
            .then(r => r.json())
            .then(body => {
              if (!body.error && body.routers) {
                state.routers = body.routers.map(sanitizeRouterColumnPaths);
                if (body.columns && body.columns.length) state.columns = body.columns;
                if (body.column_default_paths && typeof body.column_default_paths === 'object') state.columnDefaultPaths = { ...body.column_default_paths };
                loadPingMonitoringFromStorage();
              }
              renderTable();
              updateRouterFileLockUI();
            })
            .catch(() => { renderTable(); updateRouterFileLockUI(); });
        } else {
          renderTable();
          updateRouterFileLockUI();
        }
        updateRoutersFilenameDisplay();
      })
      .catch(() => {
        renderTable();
        updateRouterFileLockUI();
        updateRoutersFilenameDisplay();
      });
  }

  function saveLastFile(filename) {
    fetch('/api/config/app')
      .then(r => r.json())
      .then(cfg => {
        cfg.last_file = filename || '';
        return fetch('/api/config/app', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(cfg),
        });
      })
      .catch(() => {});
  }

  loadAppConfig();

  function newRouters() {
    state.routers = [];
    state.lastFile = '';
    updateRoutersFilenameDisplay();
    renderTable();
    showDeployStatus('New routers started. Add rows or use Discover Routers.', false);
  }

  if (btnOpen) {
    btnOpen.addEventListener('click', () => {
      fetch('/api/routers/list')
        .then(r => r.json())
        .then(body => {
          const files = body.files || [];
          openFileList.innerHTML = '';
          files.forEach(f => {
            const opt = document.createElement('option');
            opt.value = f.name;
            opt.textContent = f.name;
            openFileList.appendChild(opt);
          });
          fetch('/api/config/app')
            .then(r => r.json())
            .then(cfg => {
              const last = cfg.last_file || '';
              const idx = files.findIndex(f => f.name === last);
              openFileList.selectedIndex = idx >= 0 ? idx : 0;
            })
            .catch(() => { if (openFileList.options.length) openFileList.selectedIndex = 0; });
          openModal.classList.add('visible');
        })
        .catch(() => openModal.classList.add('visible'));
    });
  }

  if (btnOpenConfirm) {
    btnOpenConfirm.addEventListener('click', () => {
      const filename = openFileList.value;
      if (!filename) return;
      fetch('/api/routers/open?filename=' + encodeURIComponent(filename))
        .then(r => r.json())
        .then(body => {
          openModal.classList.remove('visible');
          if (body.error) {
            showDeployStatus(body.error, true);
            return;
          }
          state.routers = (body.routers || []).map(sanitizeRouterColumnPaths);
          state.columns = body.columns && body.columns.length ? body.columns : [];
          state.columnDefaultPaths = body.column_default_paths && typeof body.column_default_paths === 'object' ? { ...body.column_default_paths } : {};
          state.lastFile = filename;
          updateRoutersFilenameDisplay();
          renderTable();
          saveLastFile(filename);
          showDeployStatus('Opened: ' + filename, false);
        })
        .catch(e => {
          openModal.classList.remove('visible');
          showDeployStatus('Open failed: ' + e.message, true);
        });
    });
  }

  if (btnOpenCancel) {
    btnOpenCancel.addEventListener('click', () => openModal.classList.remove('visible'));
  }

  // Monitoring - Ping
  const btnPing = $('#btnPing');
  const pingAuto = $('#pingAuto');
  const pingInterval = $('#pingInterval');
  const pingLogOffline = $('#pingLogOffline');
  const pingLogFilename = $('#pingLogFilename');
  const pingResultsBody = $('#pingResultsBody');

  function getPingTargets() {
    const routers = state.routers.length ? collectFromTable() : state.routers;
    const targets = [];
    routers.forEach(r => {
      const v = String(r.ip_address || '').trim();
      if (!v) return;
      const ip = v.includes(':') ? v.split(':')[0] : v;
      if (ip && !targets.includes(ip)) targets.push(ip);
    });
    return targets;
  }

  function getIpToHostnameMap() {
    const routers = state.routers.length ? collectFromTable() : state.routers;
    const map = {};
    routers.forEach(r => {
      const v = String(r.ip_address || '').trim();
      if (!v) return;
      const ip = v.includes(':') ? v.split(':')[0] : v;
      const hostname = String(r.hostname || '').trim();
      if (ip && hostname && !(ip in map)) map[ip] = hostname;
    });
    return map;
  }

  function formatOfflineDuration(ms) {
    if (ms < 0) return '00:00:00';
    const sec = Math.floor(ms / 1000);
    if (sec >= 86400) {
      const days = Math.ceil(sec / 86400);
      return days + ' day' + (days !== 1 ? 's' : '');
    }
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = sec % 60;
    return [h, m, s].map(n => String(n).padStart(2, '0')).join(':');
  }

  function pingCompareVal(key, a, b, dir) {
    const numKeys = ['tx', 'rx', 'loss_pct', 'min_ms', 'avg_ms', 'max_ms'];
    let va = key === 'status' ? (a.status === 'Online' ? 1 : 0) : (a[key] ?? '');
    let vb = key === 'status' ? (b.status === 'Online' ? 1 : 0) : (b[key] ?? '');
    if (numKeys.includes(key)) {
      va = parseFloat(va) || 0;
      vb = parseFloat(vb) || 0;
    } else {
      va = String(va || '');
      vb = String(vb || '');
    }
    const cmp = va < vb ? -1 : va > vb ? 1 : 0;
    return dir * cmp;
  }

  function renderPingResults(results, sortPrimary, sortPrimaryDir, sortSecondary, sortSecondaryDir) {
    if (!pingResultsBody) return;
    const now = Date.now();
    const hostnameMap = getIpToHostnameMap();
    let rows = (results || []).map(r => {
      let statusText = r.status;
      if (r.status === 'Offline') {
        const since = state.pingOfflineSince[r.ip];
        const duration = since ? formatOfflineDuration(now - since) : '00:00:00';
        statusText = 'Offline ' + duration;
      }
      return { ...r, statusDisplay: statusText, hostname: hostnameMap[r.ip] || '-' };
    });
    if (sortPrimary !== null) {
      const keyP = sortPrimary === 'ip' ? 'ip' : sortPrimary;
      const keyS = sortSecondary !== null ? (sortSecondary === 'ip' ? 'ip' : sortSecondary) : null;
      rows.sort((a, b) => {
        const cmp = pingCompareVal(keyP, a, b, sortPrimaryDir);
        if (cmp !== 0) return cmp;
        if (keyS !== null) return pingCompareVal(keyS, a, b, sortSecondaryDir);
        return 0;
      });
    }
    const totalRows = rows.length;
    const perPage = state.pingPerPage;
    const totalPages = Math.max(1, Math.ceil(totalRows / perPage));
    state.pingPage = Math.min(state.pingPage, totalPages - 1);
    const page = Math.max(0, state.pingPage);
    const start = page * perPage;
    const pageRows = rows.slice(start, start + perPage);

    [ $('#pingPaginationTop'), $('#pingPaginationBottom') ].forEach(container => {
      if (!container) return;
      container.innerHTML = '';
      if (totalRows > 0) {
        const bar = createPaginationBar('ping', totalRows, page, perPage, (p) => {
          state.pingPage = p;
          renderPingResults(state.pingResults, sortPrimary, sortPrimaryDir, sortSecondary, sortSecondaryDir);
        }, (n) => {
          state.pingPerPage = n;
          state.pingPage = 0;
          renderPingResults(state.pingResults, sortPrimary, sortPrimaryDir, sortSecondary, sortSecondaryDir);
        });
        container.appendChild(bar);
      }
    });

    pingResultsBody.innerHTML = '';
    pageRows.forEach(r => {
      const tr = document.createElement('tr');
      if (r.status === 'Offline') tr.classList.add('offline');
      const fmtMs = v => (v !== '' && v != null) ? Math.round(parseFloat(v)).toString() : '-';
      tr.innerHTML = `<td>${escapeHtml(r.ip)}</td><td>${escapeHtml(r.hostname)}</td><td>${escapeHtml(r.statusDisplay)}</td><td>${r.tx}</td><td>${r.rx}</td><td>${r.loss_pct}%</td><td>${fmtMs(r.min_ms)}</td><td>${fmtMs(r.avg_ms)}</td><td>${fmtMs(r.max_ms)}</td>`;
      pingResultsBody.appendChild(tr);
    });

    updatePingSortArrows(sortPrimary, sortPrimaryDir);
  }

  function updatePingSortArrows(sortPrimary, sortPrimaryDir) {
    const table = document.getElementById('pingResultsTable');
    if (!table) return;
    const sortSymbol = sortPrimaryDir === 1 ? '\u2191' : '\u2193';
    table.querySelectorAll('thead th.sortable').forEach(th => {
      const arrow = th.querySelector('.sort-arrow');
      if (arrow) arrow.textContent = th.dataset.sort === sortPrimary ? ' ' + sortSymbol : '';
    });
  }

  function setPingSort(column) {
    const s = state.pingSort;
    if (s.primary === column) {
      s.primaryDir *= -1;
    } else {
      s.secondary = s.primary;
      s.secondaryDir = s.primaryDir;
      s.primary = column;
      s.primaryDir = 1;
    }
    renderPingResults(state.pingResults, s.primary, s.primaryDir, s.secondary, s.secondaryDir);
  }

  function escapeHtml(s) {
    const div = document.createElement('div');
    div.textContent = s;
    return div.innerHTML;
  }

  let pingAutoTimer = null;
  let pingDurationTickTimer = null;

  function startOfflineDurationTick() {
    stopOfflineDurationTick();
    if (Object.keys(state.pingOfflineSince).length === 0) return;
    pingDurationTickTimer = setInterval(() => {
      if (Object.keys(state.pingOfflineSince).length === 0) {
        stopOfflineDurationTick();
        return;
      }
      const s = state.pingSort;
      renderPingResults(state.pingResults, s.primary, s.primaryDir, s.secondary, s.secondaryDir);
    }, 1000);
  }

  function stopOfflineDurationTick() {
    if (pingDurationTickTimer) {
      clearInterval(pingDurationTickTimer);
      pingDurationTickTimer = null;
    }
  }

  function setPingButtonLoading(loading) {
    if (!btnPing) return;
    btnPing.disabled = loading;
    if (loading) {
      btnPing.innerHTML = '<span class="btn-spinner"></span> Pinging...';
    } else {
      btnPing.textContent = 'Ping';
    }
  }

  function runPing() {
    const targets = getPingTargets();
    if (!targets.length) {
      state.pingResults = [];
      state.pingOfflineSince = {};
      savePingMonitoringToStorage();
      stopOfflineDurationTick();
      const s = state.pingSort;
      renderPingResults([], s.primary, s.primaryDir, s.secondary, s.secondaryDir);
      return;
    }
    setPingButtonLoading(true);
    fetch('/api/monitoring/ping', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ targets, count: 5 }),
    })
      .then(r => r.json())
      .then(body => {
        if (body.error) return;
        const results = body.results || [];
        const now = Date.now();
        const prevMap = {};
        (state.pingResults || []).forEach(r => { prevMap[r.ip] = r.status; });
        const hostnameMap = getIpToHostnameMap();
        const logEnabled = pingLogOffline?.checked ?? true;
        const logFile = (pingLogFilename?.value || 'Offline Events.log').trim() || 'Offline Events.log';

        results.forEach(r => {
          if (r.status === 'Offline') {
            if (!state.pingOfflineSince[r.ip]) state.pingOfflineSince[r.ip] = now;
            if (logEnabled && prevMap[r.ip] === 'Online') {
              fetch('/api/monitoring/ping/offline-log', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ event: 'offline', ip: r.ip, hostname: hostnameMap[r.ip] || '-', filename: logFile }),
              }).catch(() => {});
            }
          } else {
            if (logEnabled && prevMap[r.ip] === 'Offline') {
              fetch('/api/monitoring/ping/offline-log', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ event: 'online', ip: r.ip, hostname: hostnameMap[r.ip] || '-', filename: logFile }),
              }).catch(() => {});
            }
            delete state.pingOfflineSince[r.ip];
          }
        });
        state.pingResults = results;
        const byIp = {};
        results.forEach(r => { byIp[r.ip] = r.status; });
        state.routers.forEach(r => {
          const ip = String(r.ip_address || '').trim().split(':')[0];
          if (ip && ip in byIp) r.state = byIp[ip];
        });
        savePingMonitoringToStorage();
        const s = state.pingSort;
        renderPingResults(state.pingResults, s.primary, s.primaryDir, s.secondary, s.secondaryDir);
        startOfflineDurationTick();
        renderTable();
      })
      .catch(() => {})
      .finally(() => setPingButtonLoading(false));
  }

  btnPing?.addEventListener('click', () => {
    runPing();
  });

  // Monitoring sub-tabs (Ping / Router API / Backup Configurations)
  const monitoringTabPing = $('#monitoringTabPing');
  const monitoringTabRemoteApi = $('#monitoringTabRemoteApi');
  const monitoringTabBackup = $('#monitoringTabBackup');
  const monitoringPanePing = $('#monitoringPanePing');
  const monitoringPaneRemoteApi = $('#monitoringPaneRemoteApi');
  const monitoringPaneBackup = $('#monitoringPaneBackup');
  const backupResultsBody = $('#backupResultsBody');

  function setMonitoringSubtab(tab) {
    [monitoringTabPing, monitoringTabRemoteApi, monitoringTabBackup].forEach(el => el?.classList.remove('active'));
    [monitoringPanePing, monitoringPaneRemoteApi, monitoringPaneBackup].forEach(el => el?.classList.remove('active'));
    if (tab === 'ping') {
      monitoringTabPing?.classList.add('active');
      monitoringPanePing?.classList.add('active');
    } else if (tab === 'remote-api') {
      monitoringTabRemoteApi?.classList.add('active');
      monitoringPaneRemoteApi?.classList.add('active');
    } else if (tab === 'backup') {
      monitoringTabBackup?.classList.add('active');
      monitoringPaneBackup?.classList.add('active');
    }
  }
  monitoringTabPing?.addEventListener('click', () => setMonitoringSubtab('ping'));
  monitoringTabRemoteApi?.addEventListener('click', () => setMonitoringSubtab('remote-api'));
  monitoringTabBackup?.addEventListener('click', () => setMonitoringSubtab('backup'));

  function computeSignalScore(rsrp, rsrq, sinr) {
    const parseNum = (v) => { const n = parseFloat(v); return typeof n === 'number' && !isNaN(n) ? n : null; };
    const r = parseNum(rsrp);
    const q = parseNum(rsrq);
    const s = parseNum(sinr);
    const rsrpVal = r ?? -140;
    const rsrqVal = q ?? -19.5;
    const rsrpScore = Math.max(0, Math.min(100, (rsrpVal + 140) / 96 * 100));
    const rsrqScore = Math.max(0, Math.min(100, (rsrqVal + 19.5) / 16.5 * 100));
    let sinrScore = 0;
    const sinrValid = s !== null;
    if (sinrValid) sinrScore = Math.max(0, Math.min(100, (s + 20) / 45 * 100));
    const combined = sinrValid ? 0.55 * sinrScore + 0.3 * rsrqScore + 0.15 * rsrpScore : (rsrpScore + rsrqScore) / 2;
    return Math.round(Math.max(0, Math.min(100, combined)));
  }

  function escapeHtml(s) {
    if (s == null) return '';
    const div = document.createElement('div');
    div.textContent = String(s);
    return div.innerHTML;
  }

  function classifyWanDevices(devicesRaw) {
    let devices = devicesRaw;
    if (typeof devices === 'string') {
      try { devices = JSON.parse(devices); } catch { return { ethernet: [], cellular: [], wwan: [] }; }
    }
    if (!devices || typeof devices !== 'object') return { ethernet: [], cellular: [], wwan: [] };
    const result = { ethernet: [], cellular: [], wwan: [] };
    for (const [deviceId, device] of Object.entries(devices)) {
      if (!device || typeof device !== 'object') continue;
      const uid = String(deviceId || '').toLowerCase();
      if (uid.startsWith('ethernet')) {
        result.ethernet.push({ deviceId, device });
      } else if (uid.startsWith('mdm') || uid.startsWith('modems')) {
        result.cellular.push({ deviceId, device });
      } else if (uid.startsWith('wwan')) {
        result.wwan.push({ deviceId, device });
      }
    }
    return result;
  }

  function parseWanDevicesToCellularSimsForExpand(devicesRaw) {
    let devices = devicesRaw;
    if (typeof devices === 'string') {
      try { devices = JSON.parse(devices); } catch { return []; }
    }
    if (!devices || typeof devices !== 'object') return [];
    const sims = [];
    for (const [deviceId, device] of Object.entries(devices)) {
      const uid = deviceId.toLowerCase();
      if ((!uid.startsWith('mdm') && !uid.startsWith('modems')) || !device || typeof device !== 'object') continue;
      const status = device.status || {};
      if (String(status.error_text || '').toUpperCase().includes('NOSIM')) continue;
      if (String(status.connection_state || '').toLowerCase() !== 'connected') continue;
      const info = device.info || {};
      const diag = device.diagnostics || {};
      const port = info.port;
      const sim = info.sim;
      const carrier = diag.CARRID;
      const dbm = diag.DBM;
      const sinr = diag.SINR;
      const rsrp = diag.RSRP;
      const rsrq = diag.RSRQ;
      const rsrp5g = diag.RSRP_5G;
      const rsrq5g = diag.RSRQ_5G;
      const sinr5g = diag.SINR_5G;
      const srvcType = String(diag.SRVC_TYPE || '').trim().toUpperCase();
      const srvcTypeDetails = String(diag.SRVC_TYPE_DETAILS || '').trim();
      let networkType;  // '4G' | '5G' | '4G/5G' (unknown)
      if (srvcType === 'LTE') networkType = '4G';
      else if (srvcType === '5G' || srvcType.includes('5G')) networkType = '5G';
      else networkType = '4G/5G';
      const has5g = rsrp5g != null && rsrq5g != null && sinr5g != null;
      const score = has5g ? computeSignalScore(rsrp5g, rsrq5g, sinr5g) : computeSignalScore(rsrp, rsrq, sinr);
      sims.push({
        deviceId,
        port,
        sim,
        carrier,
        dbm,
        rsrp,
        rsrq,
        sinr,
        rsrp_5g: rsrp5g,
        rsrq_5g: rsrq5g,
        sinr_5g: sinr5g,
        score,
        networkType,
        srvcTypeDetails,
      });
    }
    return sims;
  }

  let routersRowExpansionInterval = null;

  function stopRoutersRowExpansionInterval() {
    if (routersRowExpansionInterval) {
      clearInterval(routersRowExpansionInterval);
      routersRowExpansionInterval = null;
    }
  }

  function renderWanCardsInto(container, wanData, pingResult, offline) {
    if (!container) return;
    const c = classifyWanDevices(wanData || {});
    const eth = c.ethernet || [];
    const cellular = parseWanDevicesToCellularSimsForExpand(wanData || {});
    const wwan = c.wwan || [];
    const ethernetHtml = eth.map(({ device }) => {
      const status = device.status || {};
      const info = device.info || {};
      const conn = String(status.connection_state || '').toLowerCase();
      const connected = conn === 'connected';
      const portName = info.port_name;
      const label = typeof portName === 'string' ? portName : (portName && portName[0] != null ? String(portName[0]) : '');
      const summary = status.summary != null ? String(status.summary) : '';
      return `<div class="routers-wan-card routers-wan-ethernet">
        <div class="routers-wan-card-title">Ethernet WAN</div>
        <div class="routers-wan-ethernet-body">
          <span class="routers-wan-icon ethernet-icon ${connected ? 'connected' : 'disconnected'}" title="${connected ? 'Connected' : 'Disconnected'}">&#x1F50C;</span>
          <div class="routers-wan-card-content">
            <div class="routers-wan-label">${escapeHtml(label || 'Ethernet')}</div>
            <div class="routers-wan-summary">${escapeHtml(summary)}</div>
          </div>
        </div>
      </div>`;
    }).join('');
    const cellularHtml = cellular.map(sim => {
      const meterColor = sim.score < 25 ? 'var(--signal-red)' : sim.score < 50 ? 'var(--signal-orange)' : sim.score < 75 ? 'var(--signal-yellow)' : 'var(--signal-green)';
      const carrierLabel = sim.carrier ? escapeHtml(sim.carrier) : '—';
      let headerLabel = `${carrierLabel} ${escapeHtml(sim.networkType)}`;
      if (sim.networkType === '5G' && sim.srvcTypeDetails) {
        headerLabel += ' ' + escapeHtml(sim.srvcTypeDetails);
      }
      const rawPort = sim.port || sim.deviceId || '';
      const portDisplay = rawPort ? (String(rawPort).toLowerCase() === 'int1' ? 'Internal' : escapeHtml(String(rawPort))) : '';
      const simSlot = sim.sim != null && sim.sim !== '' ? escapeHtml(String(sim.sim)) : '';
      const simLine = [portDisplay, simSlot].filter(Boolean).join(' ');
      const rssiStr = sim.dbm != null ? `RSSI ${sim.dbm} dBm` : '';
      const sinrStr = sim.sinr != null ? `SINR ${sim.sinr} dB` : '';
      const rsrpStr = sim.rsrp != null ? `RSRP ${sim.rsrp} dBm` : '';
      const rsrqStr = sim.rsrq != null ? `RSRQ ${sim.rsrq} dB` : '';
      const line1 = [rssiStr, sinrStr].filter(Boolean).join(' · ');
      const line2 = [rsrpStr, rsrqStr].filter(Boolean).join(' · ');
      const rsrp5gStr = sim.rsrp_5g != null ? `RSRP ${sim.rsrp_5g} dBm` : '';
      const rsrq5gStr = sim.rsrq_5g != null ? `RSRQ ${sim.rsrq_5g} dB` : '';
      const sinr5gStr = sim.sinr_5g != null ? `SINR ${sim.sinr_5g} dB` : '';
      const line5g = sim.networkType === '5G' ? [rsrp5gStr, rsrq5gStr, sinr5gStr].filter(Boolean).join(' · ') : '';
      const scoreLabel = sim.networkType === '4G' ? '4G Score' : sim.networkType === '5G' ? '5G Score' : '4G/5G Score';
      const formulaTip = 'Score = 55% SINR + 30% RSRQ + 15% RSRP (if SINR available), else 50% RSRP + 50% RSRQ.';
      return `<div class="routers-wan-card routers-wan-cellular">
        <div class="routers-wan-cell-header">${headerLabel}</div>
        <div class="routers-wan-meter-label">SIGNAL METER</div>
        <div class="routers-wan-meter-wrap" title="${formulaTip}">
          <div class="routers-wan-meter-bar" style="width: ${sim.score}%; background: ${meterColor};"></div>
        </div>
        <div class="routers-wan-score" title="${formulaTip}"><span class="routers-wan-score-label">${scoreLabel}:</span> ${sim.score}</div>
        ${simLine ? `<div class="routers-wan-sim-line">${simLine}</div>` : ''}
        <div class="routers-wan-cell-stats">
          ${line1 ? `<span>${line1}</span>` : ''}
          ${line2 ? `<span>${line2}</span>` : ''}
          ${line5g ? `<span>${line5g}</span>` : ''}
        </div>
      </div>`;
    }).join('');
    const wifiHtml = wwan.map(({ device }) => {
      const status = device.status || {};
      const config = device.config || {};
      const conn = String(status.connection_state || '').toLowerCase();
      const connected = conn === 'connected';
      const trigger = config.trigger_name != null ? String(config.trigger_name) : '';
      const summary = status.summary != null ? String(status.summary) : '';
      return `<div class="routers-wan-card routers-wan-wifi">
        <div class="routers-wan-card-title">WiFi-as-WAN</div>
        <div class="routers-wan-ethernet-body">
          <span class="routers-wan-icon wifi-icon ${connected ? 'connected' : 'disconnected'}" title="${connected ? 'Connected' : 'Disconnected'}">&#x1F4F6;</span>
          <div class="routers-wan-card-content">
            <div class="routers-wan-label">${escapeHtml(trigger || 'WiFi-as-WAN')}</div>
            <div class="routers-wan-summary">${escapeHtml(summary)}</div>
          </div>
        </div>
      </div>`;
    }).join('');
    const pingLoss = pingResult?.loss_pct != null ? pingResult.loss_pct + '%' : '—';
    const pingMin = pingResult?.min_ms != null && pingResult.min_ms !== '' ? pingResult.min_ms + ' ms' : '—';
    const pingAvg = pingResult?.avg_ms != null && pingResult.avg_ms !== '' ? pingResult.avg_ms + ' ms' : '—';
    const pingMax = pingResult?.max_ms != null && pingResult.max_ms !== '' ? pingResult.max_ms + ' ms' : '—';
    const pingHtml = `<div class="routers-wan-card routers-wan-ping">
      <div class="routers-wan-card-title">Ping</div>
      <dl class="routers-wan-stats">
        <dt>Loss</dt><dd>${pingLoss}</dd>
        <dt>Min</dt><dd>${pingMin}</dd>
        <dt>Avg</dt><dd>${pingAvg}</dd>
        <dt>Max</dt><dd>${pingMax}</dd>
      </dl>
    </div>`;
    container.innerHTML = `
      <div class="routers-wan-sections">
        ${eth.length ? `<div class="routers-wan-section"><div class="routers-wan-cards">${ethernetHtml}</div></div>` : ''}
        ${cellular.length ? `<div class="routers-wan-section"><div class="routers-wan-cards">${cellularHtml}</div></div>` : ''}
        ${wwan.length ? `<div class="routers-wan-section"><div class="routers-wan-cards">${wifiHtml}</div></div>` : ''}
        <div class="routers-wan-section"><div class="routers-wan-cards">${pingHtml}</div></div>
      </div>`;
  }

  function runRoutersRowExpansionTick(routerIndex) {
    if (state.routersRowExpanded === null || state.routersRowExpanded !== routerIndex) return;
    const router = state.routers[routerIndex];
    const ip = (router?.ip_address || '').split(':')[0] || '';
    if (!ip) return;
    const expansionRow = routersBody?.querySelector(`.routers-expanded-row[data-state-row="${routerIndex}"]`);
    if (!expansionRow) return;
    const container = expansionRow.querySelector('.routers-wan-cards-wrap');
    if (!container) return;
    if (!container.querySelector('.routers-wan-sections')) container.innerHTML = '<div class="routers-wan-loading">Loading...</div>';
    fetch('/api/routers/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ routers: state.routers }),
    })
      .then(() =>
        fetch('/api/monitoring/signal-strength', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ index: routerIndex }),
        }).then(r => r.json())
      )
      .then((wanRes) => {
        if (state.routersRowExpanded !== routerIndex) return;
        const wanOk = wanRes && wanRes.ok && wanRes.data;
        if (wanOk) {
          state.routerWanData[routerIndex] = wanRes.data;
          updatePathColumnCells(routerIndex);
        }
        const currentPing = (state.pingResults || []).find(p => (p.ip || '').split(':')[0] === ip) || null;
        renderWanCardsInto(container, wanOk ? wanRes.data : null, currentPing, !wanOk);
        requestAnimationFrame(() => syncSelectTableHeights());
      })
      .catch(() => {
        if (state.routersRowExpanded === routerIndex && container) {
          renderWanCardsInto(container, null, null, true);
          requestAnimationFrame(() => syncSelectTableHeights());
        }
      });
  }

  function handleRoutersRowExpandClick(stateRowIdx) {
    if (!state.routerFileLocked) return;
    if (state.routersRowExpanded === stateRowIdx) {
      state.routersRowExpanded = null;
      stopRoutersRowExpansionInterval();
      renderTable();
      return;
    }
    if (state.routersRowExpanded != null) {
      state.routersRowExpanded = null;
      stopRoutersRowExpansionInterval();
    }
    state.routersRowExpanded = stateRowIdx;
    renderTable();
    runRoutersRowExpansionTick(stateRowIdx);
    routersRowExpansionInterval = setInterval(() => runRoutersRowExpansionTick(stateRowIdx), 3000);
  }

  // Router API
  const remoteApiMethod = $('#remoteApiMethod');
  const remoteApiPaths = $('#remoteApiPaths');
  const remoteApiPath = $('#remoteApiPath');
  const remoteApiPayload = $('#remoteApiPayload');
  const remoteApiPathsRow = $('#remoteApiPathsRow');
  const remoteApiPathRow = $('#remoteApiPathRow');
  const remoteApiPayloadRow = $('#remoteApiPayloadRow');
  const btnRemoteApiGet = $('#btnRemoteApiGet');
  const btnRemoteApiPut = $('#btnRemoteApiPut');
  const btnRemoteApiPost = $('#btnRemoteApiPost');
  const btnRemoteApiDelete = $('#btnRemoteApiDelete');
  const remoteApiResultsHead = $('#remoteApiResultsHead');
  const remoteApiResultsBody = $('#remoteApiResultsBody');
  const btnRemoteApiCopyToCsv = $('#btnRemoteApiCopyToCsv');
  const btnRemoteApiSaveToCsv = $('#btnRemoteApiSaveToCsv');
  const btnRemoteApiDownload = $('#btnRemoteApiDownload');

  let stateRemoteApiResults = { headers: [], rows: [] };
  let stateRemoteApiSelectedCol = null;

  function getRemoteApiFilename() {
    const d = new Date();
    return 'remote_api_' + d.toISOString().slice(0, 19).replace('T', '_').replace(/:/g, '.') + '.csv';
  }

  function updateRemoteApiFormVisibility() {
    const m = remoteApiMethod?.value || 'GET';
    remoteApiPathsRow.style.display = m === 'GET' ? 'block' : 'none';
    remoteApiPathRow.style.display = m !== 'GET' ? 'block' : 'none';
    remoteApiPayloadRow.style.display = (m === 'PUT' || m === 'POST') ? 'block' : 'none';
    btnRemoteApiGet.style.display = m === 'GET' ? 'block' : 'none';
    btnRemoteApiPut.style.display = m === 'PUT' ? 'block' : 'none';
    btnRemoteApiPost.style.display = m === 'POST' ? 'block' : 'none';
    btnRemoteApiDelete.style.display = m === 'DELETE' ? 'block' : 'none';
    updateRemoteApiButtonState();
  }

  function updateRemoteApiButtonState() {
    const selected = [...state.routersRowSelected].filter(i => i >= 0 && i < state.routers.length);
    const disabled = selected.length === 0;
    const btn = getRemoteApiMethodButton();
    if (btn) {
      btn.disabled = disabled;
      btn.title = disabled ? 'Select routers' : '';
    }
  }
  remoteApiMethod?.addEventListener('change', updateRemoteApiFormVisibility);
  updateRemoteApiFormVisibility();

  function getRemoteApiMethodButton() {
    const m = remoteApiMethod?.value || 'GET';
    return { GET: btnRemoteApiGet, PUT: btnRemoteApiPut, POST: btnRemoteApiPost, DELETE: btnRemoteApiDelete }[m];
  }

  function setRemoteApiButtonLoading(loading) {
    const btn = getRemoteApiMethodButton();
    if (!btn) return;
    if (loading) {
      btn.disabled = true;
    } else {
      updateRemoteApiButtonState();
    }
    const label = btn.dataset.originalText || btn.textContent;
    if (loading) {
      btn.innerHTML = '<span class="btn-spinner"></span> ' + label + 'ing...';
    } else {
      btn.textContent = label;
    }
  }

  function runRemoteApi(method) {
    collectFromTable();
    if (!state.routers.length) {
      showDeployStatus?.('Load routers file first.', true);
      return;
    }
    const selected = [...state.routersRowSelected].filter(i => i >= 0 && i < state.routers.length);
    if (!selected.length) {
      showDeployStatus?.('Select one or more routers.', true);
      return;
    }
    [btnRemoteApiGet, btnRemoteApiPut, btnRemoteApiPost, btnRemoteApiDelete].forEach(b => {
      if (b) b.dataset.originalText = b.textContent;
    });
    const data = { method, indices: selected };
    if (method === 'GET') {
      const pathsRaw = remoteApiPaths?.value || '';
      if (!pathsRaw.trim()) {
        showDeployStatus?.('Enter at least one path for GET.', true);
        return;
      }
      data.paths = pathsRaw;
    } else {
      const path = remoteApiPath?.value?.trim();
      if (!path) {
        showDeployStatus?.('Enter path.', true);
        return;
      }
      data.path = path;
      if (method === 'PUT' || method === 'POST') {
        data.payload = remoteApiPayload?.value?.trim();
      }
    }
    const allSelected = selected.length === state.routers.length;
    const doRun = () => {
      setRemoteApiButtonLoading(true);
      showDeployStatus?.('Calling routers...', false);
      fetch('/api/routers/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ routers: state.routers }),
      })
        .then(() =>
          fetch('/api/monitoring/remote-api', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data),
          })
        )
      .then(r => r.json())
      .then(body => {
        if (body.error) {
          showDeployStatus?.(body.error, true);
          return;
        }
        const cols = body.columns || [];
        const results = body.results || [];
        const csvHeaders = ['IP address', 'Hostname', ...cols.filter(c => c !== 'ip' && c !== 'hostname')];
        const csvRows = results.map(r => {
          const row = [r.ip || '', r.hostname || ''];
          cols.forEach(c => {
            if (c !== 'ip' && c !== 'hostname') row.push(r[c] ?? '');
          });
          return row;
        });
        stateRemoteApiResults = { headers: csvHeaders, rows: csvRows };
        renderRemoteApiResults(csvHeaders, csvRows);
        showDeployStatus?.('Router API complete.', false);
      })
        .catch(e => showDeployStatus?.('Router API failed: ' + e.message, true))
        .finally(() => setRemoteApiButtonLoading(false));
    };
    if (allSelected) {
      showConfirmDelete?.('Router API for all routers?', `Call ${method} on all ${selected.length} router(s)?`, doRun, '', method);
    } else {
      doRun();
    }
  }

  function renderRemoteApiResults(headers, rows) {
    if (!remoteApiResultsHead || !remoteApiResultsBody) return;
    stateRemoteApiSelectedCol = null;
    remoteApiResultsHead.innerHTML = '<tr>' + headers.map((h, i) => {
      const selectable = i >= 2;
      const metaCol = i < 2 ? 'remote-api-meta-col' : '';
      const cls = [selectable ? 'remote-api-col-selectable' : '', metaCol].filter(Boolean).join(' ');
      return `<th class="${cls}" data-col="${i}">${escapeHtml(h)}</th>`;
    }).join('') + '</tr>';

    const totalRows = rows.length;
    const perPage = state.remoteApiPerPage;
    const totalPages = Math.max(1, Math.ceil(totalRows / perPage));
    state.remoteApiPage = Math.min(state.remoteApiPage, totalPages - 1);
    const page = Math.max(0, state.remoteApiPage);
    const start = page * perPage;
    const pageRows = rows.slice(start, start + perPage);

    [ $('#remoteApiPaginationTop'), $('#remoteApiPaginationBottom') ].forEach(container => {
      if (!container) return;
      container.innerHTML = '';
      if (totalRows > 0) {
        const bar = createPaginationBar('remoteApi', totalRows, page, perPage, (p) => {
          state.remoteApiPage = p;
          renderRemoteApiResults(stateRemoteApiResults.headers, stateRemoteApiResults.rows);
        }, (n) => {
          state.remoteApiPerPage = n;
          state.remoteApiPage = 0;
          renderRemoteApiResults(stateRemoteApiResults.headers, stateRemoteApiResults.rows);
        });
        container.appendChild(bar);
      }
    });

    remoteApiResultsBody.innerHTML = pageRows.map(row =>
      '<tr>' + row.map((cell, colIdx) => {
        const metaCol = colIdx < 2 ? 'remote-api-meta-col' : '';
        return `<td${metaCol ? ` class="${metaCol}"` : ''}>${escapeHtml(String(cell ?? ''))}</td>`;
      }).join('') + '</tr>'
    ).join('');
    if (remoteApiResultsHead) {
      remoteApiResultsHead.querySelectorAll('th.remote-api-col-selectable').forEach(th => {
        th.addEventListener('click', () => {
          remoteApiResultsHead.querySelectorAll('th.remote-api-col-selected').forEach(el => el.classList.remove('remote-api-col-selected'));
          th.classList.add('remote-api-col-selected');
          stateRemoteApiSelectedCol = parseInt(th.dataset.col, 10);
        });
      });
    }
  }

  btnRemoteApiGet?.addEventListener('click', () => runRemoteApi('GET'));
  btnRemoteApiPut?.addEventListener('click', () => runRemoteApi('PUT'));
  btnRemoteApiPost?.addEventListener('click', () => runRemoteApi('POST'));
  btnRemoteApiDelete?.addEventListener('click', () => runRemoteApi('DELETE'));

  btnRemoteApiCopyToCsv?.addEventListener('click', () => {
    if (state.routerFileLocked) {
      showConfirmDelete('File is Locked', 'The router file is locked. Unlock it to add columns.', () => {}, '', 'OK');
      return;
    }
    if (stateRemoteApiSelectedCol == null) {
      showDeployStatus?.('Select a column to copy (click a column header).', true);
      return;
    }
    const { headers: apiHeaders, rows: apiRows } = stateRemoteApiResults;
    if (!apiHeaders.length || !apiRows.length) {
      showDeployStatus?.('No results to copy. Run a query first.', true);
      return;
    }
    collectFromTable();
    if (!state.routers.length) {
      showDeployStatus?.('Load routers file first.', true);
      return;
    }
    const selected = [...state.routersRowSelected].filter(i => i >= 0 && i < state.routers.length);
    if (!selected.length) {
      showDeployStatus?.('Select routers to copy to.', true);
      return;
    }
    const ipToValue = {};
    apiRows.forEach(row => {
      const ip = String(row[0] ?? '').trim().split(':')[0];
      if (ip) ipToValue[ip] = row[stateRemoteApiSelectedCol] ?? '';
    });
    const headerRaw = apiHeaders[stateRemoteApiSelectedCol];
    const key = normalizeColumnName(headerRaw) || 'column';
    const pathLike = /[./[]/.test(headerRaw);
    if (!state.columns.length) state.columns = [...DEFAULT_COLUMNS];
    if (!state.columns.some(c => normalizeColumnName(c) === key)) {
      state.columns.push(headerRaw);
    }
    if (pathLike) state.columnDefaultPaths[key] = toDotBracketPath(headerRaw);
    let rows = [...state.routers];
    const p = state.routersSort.primary;
    const s = state.routersSort.secondary;
    if (p) {
      rows.sort((a, b) => {
        const cmp = state.routersSort.primaryDir * routersCompareVal(p, a, b);
        if (cmp !== 0) return cmp;
        if (s) return state.routersSort.secondaryDir * routersCompareVal(s, a, b);
        return 0;
      });
    }
    selected.forEach(idx => {
      const r = rows[idx];
      if (!r) return;
      const ip = String(r.ip_address ?? '').trim().split(':')[0];
      r[key] = ipToValue[ip] ?? '';
    });
    fetch('/api/routers/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        routers: state.routers,
        columns: state.columns,
        column_default_paths: state.columnDefaultPaths,
      }),
    })
      .then(r => r.json())
      .then(body => {
        if (body.error) {
          showDeployStatus?.(body.error, true);
          return;
        }
        renderTable();
        showDeployStatus?.('Column "' + (headerRaw || key) + '" copied to ' + selected.length + ' router(s).', false);
      })
      .catch(e => showDeployStatus?.('Copy failed: ' + e.message, true));
  });

  btnRemoteApiSaveToCsv?.addEventListener('click', () => {
    const { headers, rows } = stateRemoteApiResults;
    if (!headers.length || !rows.length) {
      showDeployStatus?.('No results to save. Run a query first.', true);
      return;
    }
    const filename = getRemoteApiFilename();
    fetch('/api/monitoring/remote-api/save', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ headers, rows, filename }),
    })
      .then(r => r.json())
      .then(body => {
        if (body.error) {
          showDeployStatus?.(body.error, true);
          return;
        }
        showDeployStatus?.(`Saved to logs/${body.saved || filename}`, false);
        loadLogFileList?.();
      })
      .catch(e => showDeployStatus?.('Save failed: ' + e.message, true));
  });

  btnRemoteApiDownload?.addEventListener('click', () => {
    const { headers, rows } = stateRemoteApiResults;
    if (!headers.length || !rows.length) {
      showDeployStatus?.('No results to download. Run a query first.', true);
      return;
    }
    const filename = getRemoteApiFilename();
    const lines = [headers.join(',')];
    rows.forEach(row => {
      lines.push(row.map(c => `"${String(c ?? '').replace(/"/g, '""')}"`).join(','));
    });
    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    a.click();
    URL.revokeObjectURL(a.href);
    showDeployStatus?.('Download started.', false);
  });

  // Save Config
  const PING_STORAGE_KEY = 'prm_pingMonitoring';
  const DRAWER_UI_KEY = 'prm_drawerUI';
  function savePingMonitoringToStorage() {
    try {
      localStorage.setItem(PING_STORAGE_KEY, JSON.stringify({
        pingResults: state.pingResults,
        pingOfflineSince: state.pingOfflineSince,
      }));
    } catch (_) {}
  }
  function saveDrawerUI() {
    try {
      let drawerTab = 'deployment';
      if (drawerTabMonitoring?.classList.contains('active')) drawerTab = 'monitoring';
      else if (drawerTabLogs?.classList.contains('active')) drawerTab = 'logs';
      const drawerExpanded = drawer?.classList.contains('expanded');
      localStorage.setItem(DRAWER_UI_KEY, JSON.stringify({
        drawerExpanded,
        drawerTab,
        pingAuto: pingAuto?.checked ?? false,
        pingInterval: parseInt(pingInterval?.value, 10) || 30,
        pingLogOffline: pingLogOffline?.checked ?? true,
        pingLogFilename: pingLogFilename?.value || 'Offline Events.log',
        pollAuto: pollAuto?.checked ?? false,
        pollIntervalMinutes: parseInt(pollIntervalMinutes?.value, 10) || 60,
        routerFileLocked: state.routerFileLocked,
        columnDefaultPaths: state.columnDefaultPaths,
        columnDisplayNames: state.columnDisplayNames,
        routersColumnWidths: state.routersColumnWidths,
      }));
    } catch (_) {}
  }
  function loadPingMonitoringFromStorage() {
    try {
      const raw = localStorage.getItem(PING_STORAGE_KEY);
      if (raw) {
        const data = JSON.parse(raw);
        if (data.pingResults?.length) {
          state.pingResults = data.pingResults;
          state.pingOfflineSince = data.pingOfflineSince || {};
          const s = state.pingSort;
          renderPingResults(state.pingResults, s.primary, s.primaryDir, s.secondary, s.secondaryDir);
          startOfflineDurationTick();
        }
      }
      const uiRaw = localStorage.getItem(DRAWER_UI_KEY);
      if (uiRaw) {
        const ui = JSON.parse(uiRaw);
        if (pingAuto && typeof ui.pingAuto === 'boolean') pingAuto.checked = ui.pingAuto;
        if (pingInterval && ui.pingInterval) pingInterval.value = Math.max(1, ui.pingInterval);
        if (pingLogOffline && typeof ui.pingLogOffline === 'boolean') pingLogOffline.checked = ui.pingLogOffline;
        if (pingLogFilename && ui.pingLogFilename != null) pingLogFilename.value = String(ui.pingLogFilename);
        if (pollAuto && typeof ui.pollAuto === 'boolean') pollAuto.checked = ui.pollAuto;
        if (pollIntervalMinutes && ui.pollIntervalMinutes) pollIntervalMinutes.value = Math.max(1, ui.pollIntervalMinutes);
        if (typeof ui.routerFileLocked === 'boolean') state.routerFileLocked = ui.routerFileLocked;
        if (ui.columnDefaultPaths && typeof ui.columnDefaultPaths === 'object') state.columnDefaultPaths = { ...state.columnDefaultPaths, ...ui.columnDefaultPaths };
        if (ui.columnDisplayNames && typeof ui.columnDisplayNames === 'object') state.columnDisplayNames = { ...state.columnDisplayNames, ...ui.columnDisplayNames };
        if (ui.routersColumnWidths && typeof ui.routersColumnWidths === 'object') state.routersColumnWidths = { ...state.routersColumnWidths, ...ui.routersColumnWidths };
        if (pingAuto?.checked) {
          const sec = parseInt(pingInterval?.value, 10) || 30;
          pingAutoTimer = setInterval(() => runPing(), Math.max(5, sec) * 1000);
        }
      }
      clearInterval(pollAutoTimer);
      pollAutoTimer = null;
      if (pollAuto?.checked) {
        const min = parseInt(pollIntervalMinutes?.value, 10) || 60;
        pollAutoTimer = setInterval(runPollRouters, Math.max(1, min) * 60 * 1000);
      }
    } catch (_) {}
  }

  const pingResultsTable = $('#pingResultsTable');
  pingResultsTable?.addEventListener('click', (e) => {
    const th = e.target.closest('th.sortable');
    if (th) setPingSort(th.dataset.sort);
  });

  pingAuto?.addEventListener('change', () => {
    clearInterval(pingAutoTimer);
    pingAutoTimer = null;
    if (pingAuto.checked) {
      const sec = parseInt(pingInterval.value, 10) || 30;
      runPing();
      pingAutoTimer = setInterval(() => runPing(), Math.max(5, sec) * 1000);
    } else {
      stopOfflineDurationTick();
    }
    saveDrawerUI?.();
  });

  pingInterval?.addEventListener('change', () => {
    if (pingAuto?.checked && pingAutoTimer) {
      clearInterval(pingAutoTimer);
      const sec = parseInt(pingInterval.value, 10) || 30;
      pingAutoTimer = setInterval(() => runPing(), Math.max(5, sec) * 1000);
    }
    saveDrawerUI?.();
  });

  pingLogOffline?.addEventListener('change', () => saveDrawerUI?.());
  pingLogFilename?.addEventListener('change', () => saveDrawerUI?.());

  routersTable?.addEventListener('click', (e) => {
    const th = e.target.closest('th.routers-sortable');
    if (th?.dataset.field && !e.target.closest('.btn-icon') && !e.target.closest('.btn-col-delete')) setRoutersSort(th.dataset.field);
  });

  (function setupColumnDrag() {
    if (!routersTable) return;
    let dragFromIdx = -1;
    let lastDropTime = 0;
    routersTable.addEventListener('dragstart', (e) => {
      if (state.routerFileLocked) return;
      const th = e.target.closest('th.col-draggable');
      if (!th) return;
      const fields = getVisibleFields();
      const cellIndex = th.cellIndex;
      const fieldIdx = cellIndex - 1;  // skip state(0)
      if (fieldIdx < 0 || fieldIdx >= fields.length) return;
      dragFromIdx = fieldIdx;
      e.dataTransfer.effectAllowed = 'move';
      e.dataTransfer.setData('text/plain', String(fieldIdx));
      th.classList.add('col-dragging');
    });
    routersTable.addEventListener('dragend', (e) => {
      e.target.closest('th')?.classList.remove('col-dragging');
      document.querySelectorAll('th.col-drag-over').forEach(el => el.classList.remove('col-drag-over'));
      dragFromIdx = -1;
    });
    routersTable.addEventListener('dragover', (e) => {
      if (state.routerFileLocked || dragFromIdx < 0) return;
      const th = e.target.closest('th.col-draggable');
      if (!th) return;
      e.preventDefault();
      e.dataTransfer.dropEffect = 'move';
      document.querySelectorAll('th.col-drag-over').forEach(el => el.classList.remove('col-drag-over'));
      th.classList.add('col-drag-over');
    });
    routersTable.addEventListener('dragleave', (e) => {
      if (!e.relatedTarget || !routersTable.contains(e.relatedTarget)) {
        e.target.closest('th')?.classList.remove('col-drag-over');
      }
    });
    routersTable.addEventListener('drop', (e) => {
      e.preventDefault();
      if (state.routerFileLocked || dragFromIdx < 0) return;
      const th = e.target.closest('th.col-draggable');
      if (!th) return;
      const fields = getVisibleFields();
      const cellIndex = th.cellIndex;
      const toIdx = cellIndex - 1;
      if (toIdx >= 0 && toIdx < fields.length) {
        reorderColumns(dragFromIdx, toIdx);
        lastDropTime = Date.now();
      }
      th.classList.remove('col-drag-over');
      dragFromIdx = -1;
    });
    routersTable.addEventListener('click', (e) => {
      if (Date.now() - lastDropTime < 200 && e.target.closest('th.routers-sortable')) e.stopPropagation();
    }, true);
  })();

  routersUpload?.addEventListener('change', uploadRouters);
  btnDownload?.addEventListener('click', downloadRouters);
  btnNewRouters?.addEventListener('click', newRouters);
  btnSave?.addEventListener('click', saveRouters);
  btnSaveAs?.addEventListener('click', saveAsRouters);
  btnAddRow?.addEventListener('click', addRow);
  btnAddCol?.addEventListener('click', addColumn);

  if (btnBackupConfig) {
    btnBackupConfig.addEventListener('click', () => {
      collectFromTable();
      if (!state.routers.length) {
        showDeployStatus?.('Load routers file first.', true);
        return;
      }
      const indices = [...state.routersRowSelected].filter(i => i >= 0 && i < state.routers.length);
      if (!indices.length) {
        showDeployStatus?.('Select at least one router.', true);
        return;
      }
      const allSelected = indices.length === state.routers.length;
      const doBackup = () => {
        btnBackupConfig.disabled = true;
        showDeployStatus?.('Backing up configs...', false);
        fetch('/api/routers/update', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ routers: state.routers }),
        })
          .then(() =>
            fetch('/api/monitoring/save-config', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ indices }),
            })
          )
          .then(r => r.json())
          .then(body => {
            if (btnBackupConfig) btnBackupConfig.disabled = false;
            if (body.error) {
              showDeployStatus?.(body.error, true);
              return;
            }
            const results = body.results || [];
            const succeeded = results.filter(r => r.success).length;
            const failed = results.filter(r => !r.success).length;
            showDeployStatus?.(succeeded ? `Saved ${succeeded} config(s) to configs folder.${failed ? ` ${failed} failed.` : ''}` : (failed ? `Backup failed for all routers.` : 'No configs saved.'), failed > 0 && succeeded === 0);
            if (backupResultsBody) {
              backupResultsBody.innerHTML = results.map(r =>
                `<tr><td>${escapeHtml(r.ip || '')}</td><td>${escapeHtml(r.hostname || '')}</td><td>${r.success ? 'Success' : escapeHtml(r.error || 'Failed')}</td></tr>`
              ).join('');
            }
          })
          .catch(e => {
            if (btnBackupConfig) btnBackupConfig.disabled = false;
            showDeployStatus?.('Backup failed: ' + e.message, true);
          });
      };
      if (allSelected) {
        showConfirmDelete('Backup configs for all routers?', `Config will be saved from all ${state.routers.length} router(s) to the configs folder.`, doBackup, '', 'Backup');
      } else {
        doBackup();
      }
    });
  }

  if (btnRouterFileLock) {
    btnRouterFileLock.addEventListener('click', () => {
      if (state.routerFileLocked) {
        showConfirmDelete('Unlock router file?', 'You will be able to edit the routers list again.', () => {
          state.routerFileLocked = false;
          updateRouterFileLockUI();
          saveDrawerUI?.();
        }, '', 'Unlock');
      } else {
        state.routerFileLocked = true;
        updateRouterFileLockUI();
        saveDrawerUI?.();
      }
    });
  }

  btnSaveAsCancel.addEventListener('click', () => saveAsModal.classList.remove('visible'));
  btnSaveAsConfirm.addEventListener('click', doSaveAs);

  if (helpBtn) {
    helpBtn.addEventListener('click', () => {
      const helpContent = $('#helpContent');
      if (!helpContent || !helpModal) return;
      helpContent.innerHTML = '<p>Loading...</p>';
      helpModal.classList.add('visible');
      fetch('/api/user-guide')
        .then(r => r.json())
        .then(body => {
          if (body.html) helpContent.innerHTML = body.html;
          else helpContent.innerHTML = '<p>Unable to load User Guide.</p>';
        })
        .catch(() => {
          helpContent.innerHTML = '<p>Unable to load User Guide.</p>';
        });
    });
  }
  if (helpClose) helpClose.addEventListener('click', () => helpModal.classList.remove('visible'));
  if (helpModal) helpModal.addEventListener('click', (e) => { if (e.target === helpModal) helpModal.classList.remove('visible'); });

  (function initDeployUI() {
    setDeployType(state.deployType || 'licenses');
  })();
  loadAvailableFiles();
  renderTable();
  if (btnDeleteDeployFile) btnDeleteDeployFile.style.display = 'block';

  loadPingMonitoringFromStorage();
  updateRouterFileLockUI();
})();
