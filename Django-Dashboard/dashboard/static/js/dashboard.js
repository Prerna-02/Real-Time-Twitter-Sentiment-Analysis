/* Phase 5Dashboard JavaScript */
/* jQuery polling (/api/tweets/, /api/stats/ every 5s), Chart.js updates,
   entity/sentiment filters, confidence badge colouring, classify AJAX submit. */
/* Implementation added in Phase 5. */
// dashboard.jspolls /api/stats/ and /api/tweets/ every 5 s

const SENTIMENT_COLORS = {
  Positive:   '#28a745',
  Negative:   '#dc3545',
  Neutral:    '#6c757d',
  Irrelevant: '#17a2b8',
};

let pieChart = null;
let barChart = null;


// ── Chart initialisation ──────────────────────────────────────────────────────

function initCharts() {
  const pieCtx = document.getElementById('sentimentPie').getContext('2d');
  pieChart = new Chart(pieCtx, {
    type: 'pie',
    data: {
      labels:   [],
      datasets: [{ data: [], backgroundColor: [] }],
    },
    options: {
      responsive: true,
      plugins: {
        legend: { position: 'right' },
      },
    },
  });

  const barCtx = document.getElementById('entityBar').getContext('2d');
  barChart = new Chart(barCtx, {
    type: 'bar',
    data: {
      labels:   [],
      datasets: [{
        label:           'Tweets',
        data:            [],
        backgroundColor: '#0d6efd',
        borderRadius:    4,
      }],
    },
    options: {
      indexAxis:  'y',
      responsive: true,
      plugins: { legend: { display: false } },
      scales:  { x: { beginAtZero: true } },

      // Pointer cursor when hovering a bar
      onHover: function (event, elements) {
        event.native.target.style.cursor = elements.length ? 'pointer' : 'default';
      },

      // Click a bar → show entity sentiment breakdown modal
      onClick: function (event, elements) {
        if (!elements.length) return;
        const idx    = elements[0].index;
        const entity = barChart.data.labels[idx];
        showEntityDetailModal(entity);
      },
    },
  });
}


// ── Entity detail modal ───────────────────────────────────────────────────────

function renderSentimentList(distribution, total) {
  if (!distribution.length) {
    return '<li class="list-group-item text-muted small">No data</li>';
  }
  return distribution.map(function (d) {
    const pct   = total ? ((d.count / total) * 100).toFixed(1) : '0.0';
    const color = SENTIMENT_COLORS[d.label] || '#adb5bd';
    return `
      <li class="list-group-item d-flex justify-content-between align-items-center px-2">
        <span>
          <span style="display:inline-block;width:10px;height:10px;background:${color};
                       border-radius:50%;margin-right:.5rem;"></span>
          ${escapeHtml(d.label)}
        </span>
        <span><strong>${d.count.toLocaleString()}</strong>
              <span class="text-muted small">(${pct}%)</span></span>
      </li>`;
  }).join('');
}

function showEntityDetailModal(entity) {
  $.getJSON('/api/stats/?entity=' + encodeURIComponent(entity), function (data) {
    $('#entityModalTitle').text(entity + ' Sentiment Breakdown');
    $('#entityModalTotal').text(data.total.toLocaleString());
    $('#entityModalSparkAcc').text(data.spark_accuracy + '%');
    $('#entityModalVaderAcc').text(data.vader_accuracy + '%');

    $('#entityModalSparkList').html(renderSentimentList(data.spark_distribution, data.total));
    $('#entityModalVaderList').html(renderSentimentList(data.vader_distribution, data.total));

    // Stash entity name for the "drill" button
    $('#entityModalDrillBtn').data('entity', entity);

    new bootstrap.Modal(document.getElementById('entityDetailModal')).show();
  });
}


// ── Helpers ───────────────────────────────────────────────────────────────────

function escapeHtml(text) {
  return $('<div>').text(text).html();
}

function sentimentBadge(label) {
  if (!label) return '';
  const cls = 'badge-' + label.toLowerCase();
  return `<span class="badge sentiment-badge ${cls}">${escapeHtml(label)}</span>`;
}


// ── Stats + charts ────────────────────────────────────────────────────────────

function updateStats() {
  const entity = $('#entityFilter').val();
  const url    = entity
    ? `/api/stats/?entity=${encodeURIComponent(entity)}`
    : '/api/stats/';

  $.getJSON(url, function (data) {

    $('#totalTweets').text(data.total.toLocaleString());
    $('#positivePct').text(data.positive_pct + '%');
    $('#negativePct').text(data.negative_pct + '%');
    $('#topEntity').text(data.top_entities.length ? data.top_entities[0].entity : '');

    // Pie chartSpark sentiment distribution
    const pieLabels = data.spark_distribution.map(d => d.label);
    const pieCounts = data.spark_distribution.map(d => d.count);
    const pieColors = pieLabels.map(l => SENTIMENT_COLORS[l] || '#adb5bd');

    pieChart.data.labels                        = pieLabels;
    pieChart.data.datasets[0].data              = pieCounts;
    pieChart.data.datasets[0].backgroundColor   = pieColors;
    pieChart.update('none');   // 'none' skips animation on refresh

    // Bar charttop entities
    barChart.data.labels              = data.top_entities.map(d => d.entity);
    barChart.data.datasets[0].data   = data.top_entities.map(d => d.count);
    barChart.update('none');

    // Populate entity filter (preserves current selection)
    const $filter  = $('#entityFilter');
    const selected = $filter.val();
    $filter.find('option:not(:first)').remove();
    data.entities.forEach(e => {
      $filter.append(`<option value="${escapeHtml(e)}">${escapeHtml(e)}</option>`);
    });
    if (selected) $filter.val(selected);
  });
}


// ── Tweet table ───────────────────────────────────────────────────────────────

function updateTable() {
  const entity = $('#entityFilter').val();
  const url    = entity
    ? `/api/tweets/?entity=${encodeURIComponent(entity)}`
    : '/api/tweets/';

  $.getJSON(url, function (data) {
    const $tbody = $('#tweetTable tbody');
    $tbody.empty();

    if (!data.tweets.length) {
      $tbody.append('<tr><td colspan="6" class="text-center text-muted py-4">No tweets yet. Is the consumer running?</td></tr>');
      return;
    }

    data.tweets.forEach(function (t) {
      const sparkMatch = t.sentiment_spark === t.ground_truth;
      const vaderMatch = t.sentiment_vader === t.ground_truth;
      const confPct    = t.confidence_spark != null
        ? (t.confidence_spark * 100).toFixed(1) + '%'
        : '';

      $tbody.append(`
        <tr>
          <td class="tweet-cell" title="${escapeHtml(t.text || '')}">${escapeHtml((t.text || '').substring(0, 120))}</td>
          <td>${escapeHtml(t.entity || '')}</td>
          <td>${sentimentBadge(t.ground_truth)}</td>
          <td>
            ${sentimentBadge(t.sentiment_spark)}
            <span class="match-icon">${sparkMatch ? '✓' : '✗'}</span>
          </td>
          <td>${confPct}</td>
          <td>
            ${sentimentBadge(t.sentiment_vader)}
            <span class="match-icon">${vaderMatch ? '✓' : '✗'}</span>
          </td>
        </tr>
      `);
    });

    $('#lastUpdated').text('Updated ' + new Date().toLocaleTimeString());
  });
}


// ── Bootstrap ─────────────────────────────────────────────────────────────────

$(document).ready(function () {
  initCharts();
  updateStats();
  updateTable();

  setInterval(function () {
    updateStats();
    updateTable();
  }, 5000);

  // Entity filter affects BOTH the table and all stats/charts
  $('#entityFilter').on('change', function () {
    updateStats();
    updateTable();
  });

  // Modal "drill into entity" button → set dropdown + close modal
  $('#entityModalDrillBtn').on('click', function () {
    const entity = $(this).data('entity');
    if (entity) {
      $('#entityFilter').val(entity).trigger('change');
    }
    bootstrap.Modal.getInstance(document.getElementById('entityDetailModal')).hide();
  });
});