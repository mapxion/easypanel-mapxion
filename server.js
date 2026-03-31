import express from "express";
import multer from "multer";
import fs from "fs";
import path from "path";
import IORedis fr<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Xproces</title>

  <script src="https://cdn.jsdelivr.net/npm/exif-js"></script>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>

  <style>
    :root {
      --bg: #f4f6f3;
      --bg-soft: #eef2ee;
      --panel: rgba(255, 255, 255, 0.88);
      --panel-solid: #ffffff;
      --border: #dde5df;
      --text: #18211d;
      --muted: #6f7d75;
      --accent: #7aab39;
      --accent-2: #7aab39;
      --ok: #22a34a;
      --warning: #d49a11;
      --danger: #d45555;
      --shadow: 0 10px 30px rgba(24, 33, 29, 0.08);
      --radius: 22px;
    }

    * {
      box-sizing: border-box;
    }

    html, body {
      margin: 0;
      min-height: 100%;
      font-family: Arial, sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(184, 255, 26, 0.10), transparent 25%),
        linear-gradient(180deg, #f7f9f7 0%, #eef2ee 100%);
    }

    body {
      padding: 18px;
    }

    .app {
      width: min(1800px, calc(100vw - 32px));
      margin: 0 auto;
      display: grid;
      gap: 16px;
    }

    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 18px;
      background: rgba(255, 255, 255, 0.82);
      border: 1px solid rgba(221, 229, 223, 0.95);
      border-radius: 24px;
      padding: 18px 22px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(12px);
    }


    .topbar-actions {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }

    .topbar-actions .btn {
      padding: 9px 14px;
      font-size: 13px;
      border-radius: 12px;
    }

    .user-pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      min-height: 38px;
      padding: 0 12px;
      border-radius: 999px;
      background: #f4f6f4;
      border: 1px solid var(--border);
      color: var(--text);
      font-size: 13px;
      font-weight: 700;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 0;
    }

    .brand-logo img {
      height: 48px;
      width: auto;
      display: block;
    }


      display: flex;
      align-items: center;
      gap: 0;
      min-width: 0;
    }

    .brand-copy h1 {
      margin: 0;
      line-height: 1;
      letter-spacing: -0.02em;
      display: flex;
      align-items: center;
      gap: 2px;
      flex-wrap: nowrap;
    }

    .brand-copy h1 .brand-x {
      font-family: "Maiandra GD", serif;
      font-weight: 400;
      color: #7aab39;
      font-size: 72px;
      line-height: 0.9;
      display: inline-block;
    }

    .brand-copy h1 .brand-proces {
      font-family: "Myriad Pro", Arial, sans-serif;
      font-weight: 700;
      color: #000000;
      font-size: 30px;
      line-height: 1;
      display: inline-block;
      transform: none;
    }

    .brand-copy h1 .brand-com {
      font-family: "Myriad Pro", Arial, sans-serif;
      font-weight: 700;
      color: #7aab39;
      font-size: 30px;
      line-height: 1;
      display: inline-block;
      transform: none;
    }

    .brand-copy p {
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 14px;
    }

    .layout {
      display: grid;
      grid-template-columns: 1fr;
      gap: 20px;
      align-items: start;
    }

    .layout.with-data {
      grid-template-columns: 1fr;
    }

    .left-stack,
    .right-stack {
      display: grid;
      gap: 16px;
      align-content: start;
      min-width: 0;
    }

    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      overflow: hidden;
      backdrop-filter: blur(10px);
    }

    .panel-inner {
      padding: 18px;
    }

    .drop {
  width: 100%;
  min-height: 150px;
  border: 2px dashed rgba(110, 166, 0, 0.24);
  border-radius: 20px;
  display: flex;
  align-items: center;
  justify-content: center;
  text-align: center;
  padding: 12px 18px;
      background:
        linear-gradient(180deg, rgba(184, 255, 26, 0.05), rgba(184, 255, 26, 0.02)),
        #fbfcfb;
      color: var(--text);
      cursor: pointer;
      transition: 0.2s ease;
    }

    .drop.dragover {
      border-color: rgba(110, 166, 0, 0.45);
      background:
        linear-gradient(180deg, rgba(184, 255, 26, 0.08), rgba(184, 255, 26, 0.04)),
        #f9fcf6;
      
    }

    .drop-content {
      width: 100%;
      max-width: 860px;
    }

    .drop-title {
      font-size: 24px;
      font-weight: bold;
      margin-bottom: 4px;
      color: var(--text);
    }

    .drop-subtitle {
  font-size: 14px;
  color: var(--muted);
  line-height: 1.35;
  white-space: pre-line;
  min-height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  margin-bottom: 4px;
  text-align: center;
}

    
    .upload-meta {
      display: none;
      justify-content: center;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 6px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.3;
    }

    .upload-meta-item {
      white-space: nowrap;
    }

    .upload-valid {
      display: none;
      justify-content: center;
      margin-bottom: 6px;
      color: var(--ok);
      font-size: 14px;
      font-weight: 700;
      text-align: center;
    }

    .dataset-grid {
      display: none;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
      text-align: left;
      margin-bottom: 8px;
    }

    .dataset-item {
  background: rgba(255,255,255,0.82);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 6px 9px;
  min-height: 40px;
}

    .dataset-item.full {
      grid-column: 1 / -1;
    }

    .dataset-label {
  color: var(--muted);
  font-size: 9px;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  margin-bottom: 2px;
}

    .dataset-value {
  color: var(--text);
  font-size: 13px;
  font-weight: 700;
  line-height: 1.15;
  word-break: break-word;
  white-space: pre-line;
}

    .dataset-highlight {
  display: none;
  grid-column: 1 / -1;
  justify-content: center;
  margin-bottom: 0;
}

   .dataset-highlight-badge {
  display: block;
  min-height: auto;
  padding: 0;
  border: none;
  background: transparent;
  color: var(--ok);
  font-size: 14px;
  font-weight: 700;
  text-align: center;
}

    .dataset-highlight-badge.warn {
  color: var(--warning);
}

    .dataset-highlight-badge.bad {
  color: var(--danger);
}

    
    
    .top-config-row {
      display: none;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      align-items: start;
      margin: 6px 0 8px;
    }

    .top-config-card {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: flex-start;
    }

    .top-config-card .config-section-title {
      margin: 0 0 6px;
    }

    @media (max-width: 900px) {
      .top-config-row {
        grid-template-columns: 1fr;
      }
    }

    .config-section-title {
      text-align: center;
      font-size: 11px;
      font-weight: 800;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin: 4px 0 6px;
    }

    .quality-mode-wrap {
      display: none;
      justify-content: center;
      margin-bottom: 8px;
    }

    .quality-mode-group {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px;
      background: rgba(255,255,255,0.92);
      border: 1px solid var(--border);
      border-radius: 18px;
      box-shadow: none;
      flex-wrap: wrap;
      justify-content: center;
    }

    .quality-mode-item {
      position: relative;
      display: inline-flex;
    }

    .quality-mode-btn {
      border: 1px solid rgba(221, 229, 223, 0.95);
      border-radius: 999px;
      background: #ffffff;
      color: #000000;
      min-height: 36px;
      padding: 0 14px;
      font-size: 12px;
      font-weight: 800;
      cursor: pointer;
      transition: 0.18s ease;
      box-shadow: none;
    }

    .quality-mode-btn:hover {
      background: #ffffff;
      color: #000000;
      box-shadow: none;
      transform: none;
    }

    .quality-mode-btn.active {
      background: #ffffff;
      color: #7aab39;
      border-color: rgba(122, 171, 57, 0.55);
      box-shadow: none;
    }

    .quality-tooltip {
      position: absolute;
      left: 50%;
      bottom: calc(100% + 12px);
      transform: translateX(-50%) translateY(6px);
      min-width: 220px;
      max-width: 260px;
      padding: 12px 14px;
      border-radius: 14px;
      background: rgba(24, 33, 29, 0.96);
      color: #ffffff;
      font-size: 12px;
      line-height: 1.45;
      box-shadow: 0 16px 34px rgba(24, 33, 29, 0.22);
      opacity: 0;
      pointer-events: none;
      transition: 0.18s ease;
      z-index: 30;
      text-align: left;
    }

    .quality-tooltip strong {
      display: block;
      margin-bottom: 4px;
      font-size: 12px;
    }

    .quality-tooltip::after {
      content: "";
      position: absolute;
      left: 50%;
      top: 100%;
      transform: translateX(-50%);
      border-width: 7px;
      border-style: solid;
      border-color: rgba(24, 33, 29, 0.96) transparent transparent transparent;
    }

    .quality-mode-item:hover .quality-tooltip {
      opacity: 1;
      transform: translateX(-50%) translateY(0);
    }

    @media (max-width: 760px) {
      .quality-tooltip {
        display: none;
      }
    }


    .tams-link {
      color: #007bff;
      text-decoration: none;
      font-weight: 800;
    }

    .tams-link:hover {
      text-decoration: underline;
    }

    .tams-logo-inline {
      height: 16px;
      width: auto;
      display: inline-block;
      vertical-align: middle;
      margin-right: 6px;
    }

    .tams-preset-label {
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }


    .upload-actions {
      display: none;
      justify-content: center;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 6px;
    }

    .process-primary-action {
      display: none;
      justify-content: center;
      margin-top: 16px;
    }

    .process-primary-action .btn {
      min-width: 220px;
    }

    .actions {
      display: none;
      justify-content: center;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 10px;
      justify-content: center;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 10px;
    }

    .btn {
      border: none;
      border-radius: 14px;
      padding: 12px 18px;
      font-size: 14px;
      font-weight: 700;
      cursor: pointer;
      transition: 0.2s ease;
    }

    .btn-primary {
      background: #ffffff;
      color: #7aab39;
      border: 1px solid var(--border);
    }

    .btn-primary:hover {
      background: #f7f9f7;
    
      color: #7aab39;
    }

    .btn-success {
      background: #ffffff;
      color: #7aab39;
      border: 1px solid var(--border);
    }

    .btn-success:hover {
      background: #f7f9f7;
    
      color: #7aab39;
    }

    .btn-secondary {
      background: #f4f6f4;
      color: var(--text);
      border: 1px solid var(--border);
    }

    .btn-secondary:hover {
      background: #ecefec;
    }

    .btn-danger {
      background: #fff4f4;
      color: #9b3737;
      border: 1px solid #f1d7d7;
    }

    .btn-danger:hover {
      background: #ffecec;
    }

    .btn:disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }

    .price-time-box,
.job-status-box,
.future-box {
  background: rgba(255,255,255,0.76);
  border: 1px solid var(--border);
  border-radius: 18px;
  padding: 12px;
}

    .price-time-header,
    .future-header,
    .job-status-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 8px;
    }

    .price-time-title,
    .job-status-title,
    .future-title {
      font-size: 15px;
      font-weight: bold;
      color: var(--text);
    }

    .price-time-badge,
    .future-badge {
      font-size: 12px;
      border-radius: 999px;
      padding: 7px 10px;
      border: 1px solid rgba(34, 163, 74, 0.24);
      color: var(--ok);
      background: rgba(34, 163, 74, 0.08);
      white-space: nowrap;
    }

    .future-badge {
      color: var(--muted);
      background: #f5f7f5;
      border-color: var(--border);
    }

    .price-time-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10px;
  align-items: stretch;
}

    .price-card {
  background: rgba(34,163,74,0.06);
  border-radius: 10px;
  border: 1px solid rgba(34,163,74,0.25);
  padding: 6px 10px;
  min-height: 64px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 2px;
}

    .price-card-label {
  font-size: 10px;
  color: var(--ok);
}

    .price-card-value {
  font-size: 20px;
  font-weight: bold;
  color: var(--ok);
}

    .selected-quality-row {
      display: none !important;
      justify-content: center;
      margin-top: 10px;
    }

    .outputs-wrap {
      display: none;
      justify-content: center;
      margin-top: 6px;
    }


    .output-mode-wrap {
      display: none;
      justify-content: center;
      margin-top: 8px;
    }

    .output-mode-group {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px;
      background: rgba(255,255,255,0.92);
      border: 1px solid var(--border);
      border-radius: 18px;
      box-shadow: none;
      flex-wrap: wrap;
      justify-content: center;
    }

    .output-mode-btn {
      border: 1px solid rgba(221, 229, 223, 0.95);
      border-radius: 999px;
      background: #ffffff;
      color: #000000;
      min-height: 36px;
      padding: 0 14px;
      font-size: 12px;
      font-weight: 800;
      cursor: pointer;
      transition: 0.18s ease;
      box-shadow: none;
    }

    .output-mode-btn:hover {
      background: #ffffff;
      color: #000000;
      box-shadow: none;
      transform: none;
    }

    .output-mode-btn.active {
      background: #ffffff;
      color: #7aab39;
      border-color: rgba(122, 171, 57, 0.55);
      box-shadow: none;
    }

    .outputs-note {
      margin: 0 0 12px;
      text-align: center;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.45;
    }

    .outputs-section {
      margin-top: 12px;
      padding-top: 12px;
      border-top: 1px solid var(--border);
    }

    .outputs-section:first-of-type {
      margin-top: 0;
      padding-top: 0;
      border-top: none;
    }

    .outputs-section-title {
      text-align: left;
      font-size: 12px;
      font-weight: 800;
      color: var(--text);
      text-transform: uppercase;
      letter-spacing: 0.04em;
      margin-bottom: 8px;
    }

    .outputs-box {
      width: 100%;
      background: rgba(255,255,255,0.86);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 12px;
      box-shadow: none;
    }

    .outputs-title {
      text-align: center;
      font-size: 12px;
      font-weight: 800;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin-bottom: 2px;
    }

    .outputs-group {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      justify-content: center;
    }

    .output-item {
      position: relative;
      display: inline-flex;
    }

    .output-btn {
      border: 1px solid rgba(221, 229, 223, 0.95);
      border-radius: 999px;
      background: #ffffff;
      color: #000000;
      min-height: 38px;
      padding: 0 14px;
      font-size: 13px;
      font-weight: 800;
      cursor: pointer;
      transition: 0.18s ease;
      box-shadow: none;
    }

    .output-btn:hover {
      background: #ffffff;
      color: #000000;
      box-shadow: none;
      transform: none;
    }

    .output-btn.active {
      background: #ffffff;
      color: #7aab39;
      border-color: rgba(122, 171, 57, 0.55);
      box-shadow: none;
    }

    .output-tooltip {
      position: absolute;
      left: 50%;
      bottom: calc(100% + 12px);
      transform: translateX(-50%) translateY(6px);
      min-width: 220px;
      max-width: 280px;
      padding: 12px 14px;
      border-radius: 14px;
      background: rgba(24, 33, 29, 0.96);
      color: #ffffff;
      font-size: 12px;
      line-height: 1.45;
      box-shadow: 0 16px 34px rgba(24, 33, 29, 0.22);
      opacity: 0;
      pointer-events: none;
      transition: 0.18s ease;
      z-index: 30;
      text-align: left;
    }

    .output-tooltip strong {
      display: block;
      margin-bottom: 4px;
      font-size: 12px;
    }

    .output-tooltip::after {
      content: "";
      position: absolute;
      left: 50%;
      top: 100%;
      transform: translateX(-50%);
      border-width: 7px;
      border-style: solid;
      border-color: rgba(24, 33, 29, 0.96) transparent transparent transparent;
    }

    .output-item:hover .output-tooltip {
      opacity: 1;
      transform: translateX(-50%) translateY(0);
    }

    @media (max-width: 760px) {
      .output-tooltip {
        display: none;
      }
    }

    .selected-quality-badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 32px;
      padding: 0 12px;
      border-radius: 999px;
      background: rgba(184, 255, 26, 0.12);
      border: 1px solid rgba(158, 224, 0, 0.4);
      color: var(--text);
      font-size: 12px;
      font-weight: 800;
      text-align: center;
    }

    .future-text {
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
      min-height: 24px;
    }

    .progress-wrap {
      display: none;
      margin-top: 2px;
      margin-bottom: 16px;
    }

    .progress-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 10px;
      font-size: 13px;
      color: var(--muted);
    }

    .progress-bar {
      width: 100%;
      height: 14px;
      background: #edf1ed;
      border-radius: 999px;
      overflow: hidden;
      border: 1px solid var(--border);
    }

    .progress-fill {
      height: 100%;
      width: 0%;
      background: linear-gradient(90deg, var(--accent-2), var(--accent));
      transition: width 0.2s ease;
    }

    .progress-fill.error {
      background: linear-gradient(90deg, #ff7a7a, #ff5252);
    }

    .status {
      color: var(--text);
      min-height: 92px;
      white-space: pre-line;
      line-height: 1.55;
      font-size: 14px;
    }

    .process-summary {
      display: grid;
      gap: 10px;
      white-space: normal;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      align-items: stretch;
    }

    .process-summary-hero {
      background: #f9fbf9;
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 12px 14px;
      grid-column: span 1;
    }

    .process-summary-title {
      font-size: 16px;
      font-weight: 800;
      color: var(--text);
      margin-bottom: 4px;
    }

    .process-summary-subtitle {
      font-size: 13px;
      color: var(--muted);
      line-height: 1.35;
    }

    .process-summary-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }

    .process-summary-card {
      background: rgba(255,255,255,0.88);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 12px;
    }

    .process-summary-label {
      font-size: 10px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin-bottom: 4px;
    }

    .process-summary-value {
      font-size: 16px;
      color: var(--text);
      font-weight: 800;
      line-height: 1.3;
      word-break: break-word;
    }

    .process-summary-section {
      background: rgba(255,255,255,0.88);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 10px 12px;
      min-height: 88px;
    }

    .process-summary-section-title {
      font-size: 12px;
      font-weight: 800;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin-bottom: 4px;
    }

    .process-summary-list {
      display: grid;
      gap: 4px;
      font-size: 14px;
      color: var(--text);
      line-height: 1.35;
    }

    .process-summary-list .ok {
      color: var(--ok);
      font-weight: 700;
    }

    .process-summary-list .warn {
      color: var(--warning);
      font-weight: 700;
    }

    .process-summary-list .muted {
      color: var(--muted);
    }

    @media (max-width: 900px) {
      .process-summary-grid {
        grid-template-columns: 1fr;
      }
    }

    .process-selection-summary {
      display: none;
      margin-bottom: 12px;
      padding: 12px 14px;
      background: #f9fbf9;
      border: 1px solid var(--border);
      border-radius: 14px;
      color: var(--text);
      font-size: 13px;
      line-height: 1.45;
    }

    .selection-summary-title {
      font-size: 11px;
      font-weight: 800;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-bottom: 8px;
    }

    .selection-chips {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }

    .selection-chip {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 6px;
      min-height: 32px;
      padding: 0 12px;
      border-radius: 999px;
      background: #ffffff;
      color: #7aab39;
      border: 1px solid rgba(122, 171, 57, 0.35);
      box-shadow: none;
      font-size: 12px;
      font-weight: 800;
      white-space: nowrap;
    }

    .selection-chip-logo {
      height: 16px;
      width: auto;
      display: inline-block;
      vertical-align: middle;
      flex: 0 0 auto;
    }

    .process-stage-wrap {
      display: none;
      gap: 8px;
      margin-top: 8px;
      margin-bottom: 10px;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      align-items: stretch;
    }

    .process-stage {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      min-height: 34px;
      padding: 6px 10px;
      border-radius: 999px;
      background: #fafcf9;
      border: 1px solid var(--border);
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      text-align: center;
      white-space: nowrap;
    }

    .process-stage-dot {
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: #c9d3cc;
      flex: 0 0 10px;
    }

    .process-stage-body {
      flex: 1 1 auto;
      min-width: 0;
    }

    .process-stage-name {
      display: block;
    }

    .process-stage-progress {
      display: none !important;
      width: 100%;
      height: 6px;
      margin-top: 6px;
      background: #edf1ed;
      border-radius: 999px;
      overflow: hidden;
      border: 1px solid var(--border);
    }

    .process-stage-progress-fill {
      width: 0%;
      height: 100%;
      background: linear-gradient(90deg, var(--accent-2), var(--accent));
      transition: width 0.2s ease;
    }

    .process-stage.active .process-stage-progress,
    .process-stage.done .process-stage-progress {
      display: block;
    }

    .process-stage.done .process-stage-progress-fill {
      width: 100% !important;
    }

    .process-stage.active {
      color: var(--text);
      border-color: rgba(184, 255, 26, 0.65);
      background: rgba(184, 255, 26, 0.12);
    }

    .process-stage.active .process-stage-dot {
      background: #95db00;
    }

    .process-stage.done {
      color: var(--ok);
      border-color: rgba(34, 163, 74, 0.24);
      background: rgba(34, 163, 74, 0.06);
    }

    .process-stage.done .process-stage-dot {
      background: var(--ok);
    }

    .process-stage.error {
      color: var(--danger);
      border-color: rgba(212, 85, 85, 0.24);
      background: rgba(212, 85, 85, 0.06);
    }

    .process-stage.error .process-stage-dot {
      background: var(--danger);
    }

    .process-download-wrap {
      display: none;
      margin-top: 20px;
      width: 100%;
      text-align: center;
    }

    .process-download-btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 46px;
      padding: 12px 18px;
      border-radius: 14px;
      background: #ffffff;
      color: #7aab39;
      text-decoration: none;
      font-size: 14px;
      font-weight: 800;
      border: 1px solid var(--border);
      transition: 0.18s ease;
      box-shadow: none;
    }

    .process-download-btn:hover {
      background: #f7f9f7;
    
      color: #7aab39;
    }

    #mapWrap {
      display: block;
    }

    #map {
  width: 100%;
  height: 260px;
  border-radius: 18px;
  border: 1px solid var(--border);
  overflow: hidden;
  background: #edf2ee;
}


    .jobs-panel-hidden {
      display: none !important;
    }

    .jobs-list {
      display: grid;
      gap: 10px;
    }

    .job-card {
      background: rgba(255,255,255,0.82);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 12px;
      cursor: pointer;
      transition: 0.18s ease;
    }

    .job-card:hover {
      
      box-shadow: 0 8px 18px rgba(24, 33, 29, 0.08);
      border-color: rgba(158, 224, 0, 0.45);
    }

    .job-card.active {
      border-color: rgba(158, 224, 0, 0.65);
      background: rgba(184, 255, 26, 0.08);
      box-shadow: 0 8px 18px rgba(158, 224, 0, 0.12);
    }

    .job-card-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 6px;
      flex-wrap: wrap;
    }

    .job-card-title {
      font-size: 14px;
      font-weight: 800;
      color: var(--text);
    }
    .job-card-status-badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 28px;
      padding: 0 10px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 800;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      border: 1px solid var(--border);
      background: #f6f8f6;
      color: var(--muted);
      white-space: nowrap;
    }

    .job-card-status-badge.ok {
      background: rgba(34, 163, 74, 0.08);
      color: var(--ok);
      border-color: rgba(34, 163, 74, 0.22);
    }

    .job-card-status-badge.error {
      background: rgba(212, 85, 85, 0.08);
      color: var(--danger);
      border-color: rgba(212, 85, 85, 0.22);
    }

    .job-card-status-badge.progress {
      background: rgba(122, 171, 57, 0.10);
      color: var(--accent);
      border-color: rgba(122, 171, 57, 0.25);
    }


    .job-card-id {
      font-size: 12px;
      color: var(--muted);
      word-break: break-all;
    }

    .job-card-meta {
      font-size: 12px;
      color: var(--muted);
      line-height: 1.4;
    }

    .job-empty {
      color: var(--muted);
      font-size: 14px;
      text-align: center;
      padding: 8px 0;
    }

    .footer-note {
      font-size: 12px;
      color: var(--muted);
      text-align: center;
      padding: 6px 0 0;
    }

    input[type="file"] {
      display: none;
    }

    .modal-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(16, 24, 18, 0.18);
      backdrop-filter: blur(8px);
      display: none;
      align-items: center;
      justify-content: center;
      padding: 20px;
      z-index: 9999;
    }

    .modal-backdrop.show {
      display: flex;
    }

    .modal {
      width: min(100%, 560px);
      max-width: 560px;
      max-height: min(90vh, 820px);
      background: rgba(255,255,255,0.94);
      border: 1px solid var(--border);
      border-radius: 24px;
      box-shadow: 0 24px 60px rgba(24, 33, 29, 0.14);
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }

    .modal-header {
      padding: 20px 22px 14px;
      border-bottom: 1px solid var(--border);
    }

    .modal-title {
      margin: 0;
      font-size: 22px;
      color: var(--text);
    }

    .modal-subtitle {
      margin-top: 6px;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.45;
    }

    .modal-body {
      padding: 18px 22px;
      overflow-y: auto;
      flex: 1 1 auto;
      min-height: 0;
    }

    .modal-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      margin-bottom: 14px;
    }

    .modal-card {
      background: #fafcf9;
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 14px;
    }

    .modal-card.full {
      grid-column: 1 / -1;
    }

    .modal-label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 6px;
    }

    .modal-value {
      color: var(--text);
      font-size: 18px;
      font-weight: bold;
      word-break: break-word;
    }

    .modal-note {
      background: #f9fbf9;
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 14px;
      color: var(--text);
      font-size: 14px;
      line-height: 1.5;
      white-space: pre-line;
    }

    .modal-footer {
      display: flex;
      justify-content: flex-end;
      gap: 10px;
      padding: 16px 22px 22px;
      border-top: 1px solid var(--border);
      background: rgba(255,255,255,0.98);
      flex: 0 0 auto;
    }

    .modal-warning .modal-title {
      color: var(--warning);
    }

    .modal-error .modal-title {
      color: var(--danger);
    }

    #analysisPanel { display: none !important; }

    @media (max-width: 760px) {
  .layout,
  .layout.with-data {
    grid-template-columns: 1fr;
  }

  #map {
    height: 300px;
  }
}

    @media (max-width: 760px) {
      body {
        padding: 12px;
      }

      .topbar {
        padding: 14px;
      }

      .brand-copy h1 {
      align-items: center;
        gap: 1px;
      }

      .brand-copy h1 .brand-x {
        font-size: 54px;
      }

      .brand-copy h1 .brand-proces,
      .brand-copy h1 .brand-com {
      display: inline-block;
      transform: translateY(6px);
        font-size: 22px;
      }

      .dataset-grid,
      .price-time-grid,
      .modal-grid {
        grid-template-columns: 1fr;
      }

      .process-stage-wrap {
        grid-template-columns: 1fr 1fr;
      }

      .process-summary {
        grid-template-columns: 1fr;
      }

      .drop {
        min-height: 150px;
      }

      .drop-title {
        font-size: 22px;
      }

      .drop-subtitle {
        min-height: 24px;
        font-size: 14px;
      }

      #map {
        height: 220px;
      }

      .modal-footer {
        flex-direction: column;
      }

      .modal-footer .btn {
        width: 100%;
      }
    }
  </style>
</head>
<body>
  <div class="app">
    <div class="topbar">
      <div class="brand">
        <div class="brand-copy">
          <div class="brand-logo"><img src="xproces.png476por176.png" alt="Xproces"></div>
          <p>Procesamiento de datos fotogramétricos</p>
        </div>
      </div>
      <div class="topbar-actions" id="topbarActions">
        <button class="btn btn-secondary" id="loginBtn" type="button">Iniciar sesión</button>
        <button class="btn btn-primary" id="registerBtn" type="button">Registrarse</button>
        <button class="btn btn-secondary" id="inviteBtn" type="button">Código invitado</button>
      </div>
    </div>

    <div class="layout" id="mainLayout">
      <div class="left-stack">
<div class="panel" id="uploadPanel">
          <div class="panel-inner">
            <div class="drop" id="drop">
              <div class="drop-content">
                <div class="drop-title" id="dropTitle">Arrastra tus fotos aquí</div>

                <div class="drop-subtitle" id="dropSubtitle">
                  para analizarlas, calcular tiempo y precio y configurar el procesado.
                </div>

                
                <div class="upload-actions" id="uploadActions">
                  <button class="btn btn-danger" id="clearBtn" type="button">Eliminar fotos</button>
                </div>

<input id="fileInput" type="file" multiple accept=".jpg,.jpeg,.png,.tif,.tiff,.dng" />

            </div>
            </div>
          </div>
        </div>


<div class="panel" id="datasetPanel" style="display:none;">
          <div class="panel-inner">
<div class="dataset-grid" id="datasetGrid">
                  <div class="dataset-item">
                    <div class="dataset-label">Tamaño total</div>
                    <div class="dataset-value" id="fieldSize">0 MB</div>
                  </div>

                  <div class="dataset-item">
                    <div class="dataset-label">Resolución media</div>
                    <div class="dataset-value" id="fieldResolution">Sin detectar</div>
                  </div>

                  <div class="dataset-item">
                  <div class="dataset-label">Lente</div>
                  <div class="dataset-value" id="fieldLens">Sin detectar</div>
                </div>

                <div class="dataset-item">
                  <div class="dataset-label">Focal</div>
                  <div class="dataset-value" id="fieldFocal">Sin detectar</div>
                </div>

                <div class="dataset-item">
                  <div class="dataset-label">Fecha</div>
                  <div class="dataset-value" id="fieldDate">Sin detectar</div>
                </div>
                  
                  <div class="dataset-item">
                    <div class="dataset-label">GPS</div>
                    <div class="dataset-value" id="fieldGps">Sin detectar</div>
                  </div>

                  <div class="dataset-item">
                    <div class="dataset-label">Cámara</div>
                    <div class="dataset-value" id="fieldCamera">Sin detectar</div>
                  </div>

                  <div class="dataset-item">
                    <div class="dataset-label">Equipo de captura</div>
                    <div class="dataset-value" id="fieldDrone">Sin detectar</div>
                  </div>

                  <div class="dataset-item">
                    <div class="dataset-label">Tipo de misión</div>
                    <div class="dataset-value" id="fieldMission">Sin detectar</div>
                  </div>

                  <div class="dataset-item">
                    <div class="dataset-label">Solape</div>
                    <div class="dataset-value" id="fieldOverlap">Sin detectar</div>
                  </div>

                 
                <div class="dataset-item full" style="display:none;">
  <div class="dataset-label">Ubicación</div>
  <div class="dataset-value" id="fieldLocation">Sin detectar</div>
</div>

<div class="dataset-highlight" id="datasetHighlight" style="display:none;">
  <div class="dataset-highlight-badge" id="datasetHighlightBadge">
    Dataset válido para fotogrametría
  </div>
</div>
                </div>

                
                <div class="top-config-row" id="topConfigRow">
                  <div class="top-config-card">
                    <div class="config-section-title">Calidad</div>
                    <div class="quality-mode-wrap" id="qualityInlineRow" style="display:flex; margin:0;">
                      <div class="quality-mode-group" role="group" aria-label="Calidad de procesado">
                        <input id="qualityMode" type="hidden" value="" />

                        <div class="quality-mode-item">
                          <button class="quality-mode-btn" type="button" data-quality-mode="fast">Rápido</button>
                          <div class="quality-tooltip">
                            <strong>Rápido</strong>
                            Procesado más veloz y económico. Recomendado para proyectos en los que no se necesite un detalle perfecto y prime la velocidad del procesado.
                          </div>
                        </div>

                        <div class="quality-mode-item">
                          <button class="quality-mode-btn" type="button" data-quality-mode="normal">Normal</button>
                          <div class="quality-tooltip">
                            <strong>Normal</strong>
                            Equilibrio entre tiempo, coste y detalle. Es la opción recomendada para la mayoría de proyectos.
                          </div>
                        </div>

                        <div class="quality-mode-item">
                          <button class="quality-mode-btn" type="button" data-quality-mode="max">Máxima calidad</button>
                          <div class="quality-tooltip">
                            <strong>Máxima calidad</strong>
                            Procesado más detallado y más lento. Pensado para obtener el mejor resultado posible cuando prima la precisión.
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div class="top-config-card">
                    <div class="config-section-title">Entrega</div>
                    <div class="output-mode-wrap" id="outputInlineRow" style="display:flex; margin:0;">
                      <div class="output-mode-wrap" id="outputModeWrap" style="display:flex; margin:0;">
                        <div class="output-mode-group" role="group" aria-label="Modo de selección de archivos de salida">
                          <button class="output-mode-btn" type="button" data-output-mode="preset">Paquetes recomendados</button>
                          <button class="output-mode-btn" type="button" data-output-mode="manual">Selección manual</button>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>

<div class="outputs-wrap" id="outputsWrap">
                  <div class="outputs-box">
                    <div class="outputs-title">Archivos de salida</div>
                    <div class="outputs-note" id="outputsModeNote">
                      Paquetes prediseñados
                    </div>

                    
                    <div id="presetOutputsPanel">
                      <div class="outputs-section">
                        <div class="outputs-group">
                          <div class="output-item">
                            <button class="output-btn" type="button" data-preset="inspection_standard">Inspección estándar</button>
                            <div class="output-tooltip"><strong>Inspección estándar</strong>Paquete recomendado para trabajo técnico general. Incluye una nube principal en LAS, una malla OBJ y un informe PDF.</div>
                          </div>

                          <div class="output-item">
                            <button class="output-btn" type="button" data-preset="inspection_tams" aria-label="TAMS">
                              <img src="data:image/png;base64,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" alt="TAMS" class="tams-logo-inline" style="height:20px; margin-right:0;">
                            </button>
                            <div class="output-tooltip"><strong><a class="tams-link" href="https://www.intelsi.es/tams/" target="_blank" rel="noopener">TAMS</a></strong>Paquete base para inspección en TAMS. Incluye la nube LAS necesaria para tu flujo TAMS.</div>
                          </div>

                          <div class="output-item">
                            <button class="output-btn" type="button" data-preset="web_delivery">Web</button>
                            <div class="output-tooltip"><strong>Web</strong>Paquete orientado a visualización web. Incluye Cesium 3D Tiles y un informe PDF.</div>
                          </div>

                          <div class="output-item">
                            <button class="output-btn" type="button" data-preset="compact_delivery">Entrega compacta</button>
                            <div class="output-tooltip"><strong>Entrega compacta</strong>Paquete pensado para entregar archivos más ligeros. Incluye LAZ comprimido, una malla OBJ y un informe PDF.</div>
                          </div>
                        </div>
                      </div>
                    </div>

<div id="manualOutputsPanel" style="display:none;">
                      <div class="outputs-section">
                        <div class="outputs-section-title">Formatos de nube de puntos</div>
                        <div class="outputs-group">
                          <div class="output-item"><button class="output-btn" type="button" data-output="las">LAS</button><div class="output-tooltip"><strong>LAS</strong>Nube de puntos estándar muy usada en inspección, topografía, CAD y GIS.</div></div>
                          <div class="output-item"><button class="output-btn" type="button" data-output="laz">LAZ</button><div class="output-tooltip"><strong>LAZ</strong>La misma nube que LAS, pero comprimida para ocupar menos espacio.</div></div>
                          <div class="output-item"><button class="output-btn" type="button" data-output="e57">E57</button><div class="output-tooltip"><strong>E57</strong>Formato profesional de nube de puntos para compatibilidad con software de ingeniería.</div></div>
                          <div class="output-item"><button class="output-btn" type="button" data-output="ply">PLY</button><div class="output-tooltip"><strong>PLY</strong>Formato técnico 3D muy usado en software especializado.</div></div>
                          <div class="output-item"><button class="output-btn" type="button" data-output="pts">PTS</button><div class="output-tooltip"><strong>PTS</strong>Formato sencillo de nube de puntos usado en algunos flujos técnicos.</div></div>
                        </div>
                      </div>

                      <div class="outputs-section">
                        <div class="outputs-section-title">Modelos 3D y mallas</div>
                        <div class="outputs-group">
                          <div class="output-item"><button class="output-btn" type="button" data-output="mesh_obj">Malla OBJ</button><div class="output-tooltip"><strong>Malla OBJ</strong>Modelo 3D de superficie con textura, muy compatible con programas 3D.</div></div>
                          <div class="output-item"><button class="output-btn" type="button" data-output="mesh_fbx">FBX</button><div class="output-tooltip"><strong>FBX</strong>Formato muy usado en visualización, animación y programas 3D.</div></div>
                          <div class="output-item"><button class="output-btn" type="button" data-output="mesh_stl">STL</button><div class="output-tooltip"><strong>STL</strong>Formato clásico de malla, conocido por impresión 3D e ingeniería.</div></div>
                          <div class="output-item"><button class="output-btn" type="button" data-output="mesh_dae">DAE</button><div class="output-tooltip"><strong>DAE</strong>Formato Collada para intercambio entre distintos programas 3D.</div></div>
                          <div class="output-item"><button class="output-btn" type="button" data-output="mesh_3ds">3DS</button><div class="output-tooltip"><strong>3DS</strong>Formato 3D clásico, útil sobre todo por compatibilidad antigua.</div></div>
                          <div class="output-item"><button class="output-btn" type="button" data-output="mesh_wrl">WRL</button><div class="output-tooltip"><strong>WRL</strong>Formato 3D antiguo pero todavía utilizable en algunos flujos concretos.</div></div>
                          <div class="output-item"><button class="output-btn" type="button" data-output="mesh_ply">Malla PLY</button><div class="output-tooltip"><strong>Malla PLY</strong>Otra forma de exportar la superficie 3D para intercambio técnico.</div></div>
                        </div>
                      </div>

                      <div class="outputs-section">
                        <div class="outputs-section-title">Ortofotos, elevación y documentación</div>
                        <div class="outputs-group">
                          <div class="output-item"><button class="output-btn" type="button" data-output="orthomosaic_tif">Ortomosaico TIFF</button><div class="output-tooltip"><strong>Ortomosaico TIFF</strong>Imagen corregida geométricamente, útil para una vista cenital precisa.</div></div>
                          <div class="output-item"><button class="output-btn" type="button" data-output="orthomosaic_jpg">Ortomosaico JPG</button><div class="output-tooltip"><strong>Ortomosaico JPG</strong>Versión más ligera del ortomosaico, buena para consulta rápida.</div></div>
                          <div class="output-item"><button class="output-btn" type="button" data-output="dem_tif">DEM/DSM TIFF</button><div class="output-tooltip"><strong>DEM/DSM TIFF</strong>Modelo de elevación más útil en trabajos topográficos y cartográficos.</div></div>
                          <div class="output-item"><button class="output-btn" type="button" data-output="pdf_report">Informe PDF</button><div class="output-tooltip"><strong>Informe PDF</strong>Resumen del proyecto y del procesado para documentación y entrega.</div></div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
          </div>
                  </div>
                </div>
          </div>
        </div>

<div class="panel" id="pricingPanel" style="display:none;">
          <div class="panel-inner">
            <div class="price-time-box">
              <div class="price-time-grid">
                <div class="price-card">
                  <div class="price-card-label">Precio</div>
                  <div class="price-card-value" id="priceBig">0 €</div>
                </div>

                <div class="price-card">
                  <div class="price-card-label">Tiempo estimado</div>
                  <div class="price-card-value" id="etaBig">Sin calcular</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div class="right-stack">
<div class="panel" id="processPanel" style="display:none;">
          <div class="panel-inner">
            <div class="job-status-box">
              <div class="job-status-header">
                <div>
                  <div id="projectTitle" style="display:none; font-weight:700; margin-bottom:4px;"></div>
                  <div class="job-status-title">Estado del procesado</div>
                </div>
              </div>

              
            <div class="progress-wrap" id="progressWrap">
              <div class="progress-top">
                <span id="progressLabel">Procesando...</span>
                <span id="progressPercent">0%</span>
              </div>
              <div class="progress-bar">
                <div class="progress-fill" id="progressFill"></div>
              </div>
            </div>
<div class="process-stage-wrap" id="processStageWrap">
                <div class="process-stage" data-stage="matching">
                  <div class="process-stage-dot"></div>
                  <div>Alineación inicial</div>
                </div>
                <div class="process-stage" data-stage="aligning">
                  <div class="process-stage-dot"></div>
                  <div>Alineando cámaras</div>
                </div>
                <div class="process-stage" data-stage="depth">
                  <div class="process-stage-dot"></div>
                  <div>Mapas de profundidad</div>
                </div>
                <div class="process-stage" data-stage="pointcloud">
                  <div class="process-stage-dot"></div>
                  <div>Nube de puntos</div>
                </div>
                <div class="process-stage" data-stage="export">
                  <div class="process-stage-dot"></div>
                  <div>Exportación</div>
                </div>
              </div>

              <div class="process-selection-summary" id="processSelectionSummary"></div>

              <div class="status" id="jobStatus">Aquí se mostrará el estado real del trabajo una vez iniciado.</div>

              <div class="process-download-wrap" id="processDownloadWrap">
                <a class="process-download-btn" id="processDownloadBtn" href="#" target="_blank">Descargar resultados</a>
              </div>

              <div class="process-primary-action" id="processStartWrap">
                <button class="btn btn-primary" id="uploadBtn" type="button">Iniciar procesado</button>
              </div>
            </div>
          </div>
        </div>

<div class="panel" id="mapWrap" style="display:none;">
          <div class="panel-inner">
            <div class="dataset-label" style="margin-bottom: 10px;">Situación de la toma de datos</div>
            <div id="map"></div>
          </div>
        </div>

<div class="panel" id="analysisPanel">
          <div class="panel-inner">
            <div class="future-box">
              <div class="future-header">
                <div class="future-title">Análisis del dataset</div>
                <div class="future-badge" id="analysisBadge">Próximamente</div>
              </div>
              <div class="future-text" id="status"></div>
            </div>
          </div>
        </div>
      </div>
      </div>
    </div>


    <div class="panel jobs-panel-hidden" id="jobsPanel">
      <div class="panel-inner">
        <div class="job-status-box">
          <div class="job-status-header">
            <div class="job-status-title">Tus trabajos</div>
          </div>
          <div class="jobs-list" id="jobsList"></div>
          <div class="job-empty" id="jobsEmpty">Todavía no hay trabajos guardados en esta cuenta.</div>
        </div>
      </div>
    </div>

    <div class="footer-note">
     Un proyecto de Intelsi Telecomunicaciones, S.L. Rellinars, Barcelona
    </div>
  </div>

  <div class="modal-backdrop" id="modalBackdrop">
    <div class="modal" id="modalBox">
      <div class="modal-header">
        <h2 class="modal-title" id="modalTitle">Confirmación</h2>
        <div class="modal-subtitle" id="modalSubtitle"></div>
      </div>

      <div class="modal-body" id="modalBody"></div>

      <div class="modal-footer">
        <button class="btn btn-secondary" id="modalCancelBtn" type="button">Cancelar</button>
        <button class="btn btn-success" id="modalConfirmBtn" type="button">Continuar</button>
      </div>
    </div>
  </div>

  <div class="modal-backdrop" id="authBackdrop">
    <div class="modal">
      <div class="modal-header">
        <h2 class="modal-title" id="authTitle">Acceso</h2>
        <div class="modal-subtitle" id="authSubtitle">Identifíquese para asociar sus trabajos.</div>
      </div>

      <div class="modal-body">
        <div class="modal-grid">
          <div class="modal-card full">
            <div class="modal-label">Email</div>
            <input id="authEmail" type="email" placeholder="cliente@empresa.com" style="width:100%; min-height:44px; border:1px solid var(--border); border-radius:12px; padding:0 12px; font-size:14px;">
          </div>

          <div class="modal-card full" id="authNameWrap">
            <div class="modal-label">Nombre o empresa</div>
            <input id="authName" type="text" placeholder="Nombre del cliente o empresa" style="width:100%; min-height:44px; border:1px solid var(--border); border-radius:12px; padding:0 12px; font-size:14px;">
          </div>

          <div class="modal-card full" id="authPasswordWrap">
            <div class="modal-label">Contraseña</div>
            <input id="authPassword" type="password" placeholder="Mínimo 6 caracteres" style="width:100%; min-height:44px; border:1px solid var(--border); border-radius:12px; padding:0 12px; font-size:14px;">
          </div>

          <div class="modal-card full" id="authCodeWrap" style="display:none;">
            <div class="modal-label">Código invitado</div>
            <input id="authCode" type="text" placeholder="Introduzca su código" style="width:100%; min-height:44px; border:1px solid var(--border); border-radius:12px; padding:0 12px; font-size:14px; text-transform:uppercase;">
          </div>
        </div>
      </div>

      <div class="modal-footer">
        <button class="btn btn-secondary" id="authCancelBtn" type="button">Cancelar</button>
        <button class="btn btn-success" id="authSaveBtn" type="button">Continuar</button>
      </div>
    </div>
  </div>

  <script>
    const API_BASE = "https://api.xproces.com";

function syncStoredUserToForm() {
  const user = getStoredUser && getStoredUser();
  if (!user) return;
  if (typeof authEmail !== 'undefined' && authEmail) authEmail.value = user.email || "";
  if (typeof authName !== 'undefined' && authName) authName.value = user.name || "";
}


function stripNullCharsDeep(value) {
  if (typeof value === "string") {
    return value.replace(/\u0000/g, "").replace(/\x00/g, "").replace(/\0/g, "");
  }
  if (Array.isArray(value)) {
    return value.map(stripNullCharsDeep);
  }
  if (value && typeof value === "object") {
    const out = {};
    for (const [key, val] of Object.entries(value)) {
      out[key] = stripNullCharsDeep(val);
    }
    return out;
  }
  return value;
}


    const BATCH_SIZE = 3;
    const POLL_INTERVAL_MS = 3000;
    const DATASET_SAMPLE_SIZE = 8;
    const PREVIEW_DEBOUNCE_MS = 400;

    let files = [];
    let totalSize = 0;
    let map = null;
    let baseLayer = null;
    let marker = null;
    let pollTimer = null;
    let mapResizeTimer = null;
    let previewTimer = null;
    let lastPreview = null;
    let previewRequestId = 0;
    let modalResolver = null;

    let latestDataset = null;
    let latestExifSummary = {};
    let latestMissionType = "Sin detectar";
    let pendingProjectName = "";
    let selectedHistoryJobId = "";

    const mainLayout = document.getElementById("mainLayout");
    const drop = document.getElementById("drop");
    const uploadPanel = document.getElementById("uploadPanel");
    const dropTitle = document.getElementById("dropTitle");
    const dropSubtitle = document.getElementById("dropSubtitle");
    const uploadValid = document.getElementById("uploadValid");
    const uploadMeta = document.getElementById("uploadMeta");
    const uploadPhotosMeta = document.getElementById("uploadPhotosMeta");
    const uploadGpsMeta = document.getElementById("uploadGpsMeta");
    const topConfigRow = document.getElementById("topConfigRow");
    const qualityInlineRow = document.getElementById("qualityInlineRow");
    const outputInlineRow = document.getElementById("outputInlineRow");
    const datasetGrid = document.getElementById("datasetGrid");
    const datasetHighlight = document.getElementById("datasetHighlight");
    const datasetHighlightBadge = document.getElementById("datasetHighlightBadge");

    const fieldSize = document.getElementById("fieldSize");
    const fieldResolution = document.getElementById("fieldResolution");
    const fieldGps = document.getElementById("fieldGps");
    const fieldCamera = document.getElementById("fieldCamera");
    const fieldLens = document.getElementById("fieldLens");
    const fieldFocal = document.getElementById("fieldFocal");
    const fieldDate = document.getElementById("fieldDate");
    const fieldDrone = document.getElementById("fieldDrone");
    const fieldMission = document.getElementById("fieldMission");
    const fieldOverlap = document.getElementById("fieldOverlap");
    const fieldLocation = document.getElementById("fieldLocation");

    const fileInput = document.getElementById("fileInput");
    const uploadBtn = document.getElementById("uploadBtn");
    const clearBtn = document.getElementById("clearBtn");
    const uploadActions = document.getElementById("uploadActions");
    const datasetPanel = document.getElementById("datasetPanel");
    const processStartWrap = document.getElementById("processStartWrap");
    const jobsPanel = document.getElementById("jobsPanel");
    const jobsList = document.getElementById("jobsList");
    const jobsEmpty = document.getElementById("jobsEmpty");
    const projectNameInput = document.getElementById("projectName");
    const statusEl = document.getElementById("status");
    const jobStatusEl = document.getElementById("jobStatus");
    const projectTitle = document.getElementById("projectTitle");
    const analysisBadgeEl = document.getElementById("analysisBadge");

    const priceBig = document.getElementById("priceBig");
    const etaBig = document.getElementById("etaBig");
    const selectedQualityRow = null;
    const selectedQualityBadge = null;

    const progressWrap = document.getElementById("progressWrap");
    const progressFill = document.getElementById("progressFill");
    const progressLabel = document.getElementById("progressLabel");
    const progressPercent = document.getElementById("progressPercent");

    const processStageWrap = document.getElementById("processStageWrap");
    const processDownloadWrap = document.getElementById("processDownloadWrap");
    const processDownloadBtn = document.getElementById("processDownloadBtn");

    const pricingPanel = document.getElementById("pricingPanel");
    const processPanel = document.getElementById("processPanel");
    const mapWrap = document.getElementById("mapWrap");
    const qualityModeInput = document.getElementById("qualityMode");
    const qualityModeWrap = document.querySelector(".quality-mode-wrap");
    const outputsWrap = document.getElementById("outputsWrap");
    const outputModeWrap = document.getElementById("outputModeWrap");

    const modalBackdrop = document.getElementById("modalBackdrop");
    const modalBox = document.getElementById("modalBox");
    const modalTitle = document.getElementById("modalTitle");
    const modalSubtitle = document.getElementById("modalSubtitle");
    const modalBody = document.getElementById("modalBody");
    const modalCancelBtn = document.getElementById("modalCancelBtn");
    const modalConfirmBtn = document.getElementById("modalConfirmBtn");

    const topbarActions = document.getElementById("topbarActions");
    const authBackdrop = document.getElementById("authBackdrop");
    const authTitle = document.getElementById("authTitle");
    const authSubtitle = document.getElementById("authSubtitle");
    const authEmail = document.getElementById("authEmail");
    const authName = document.getElementById("authName");
    const authPassword = document.getElementById("authPassword");
    const authNameWrap = document.getElementById("authNameWrap");
    const authCode = document.getElementById("authCode");
    const authPasswordWrap = document.getElementById("authPasswordWrap");
    const authCodeWrap = document.getElementById("authCodeWrap");
    const authCancelBtn = document.getElementById("authCancelBtn");
    const authSaveBtn = document.getElementById("authSaveBtn");


    let authMode = "login";


    
    // INIT botones calidad
    document.querySelectorAll(".quality-mode-btn").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        setSelectedQualityMode(btn.dataset.qualityMode || "normal", true);
      });
    });

    document.querySelectorAll(".output-mode-btn").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        setSelectedOutputMode(btn.dataset.outputMode || "preset");
        refreshPricingPreviewDebounced();
      });
    });

    document.querySelectorAll("[data-preset]").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (getSelectedOutputMode() !== "preset") {
          setSelectedOutputMode("preset");
        }
        setSelectedPreset(btn.dataset.preset || "");
        updateOutputsModeNote();
        updateProcessSelectionSummary();
        refreshPricingPreviewDebounced();
      });
    });

    document.querySelectorAll("[data-output]").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (getSelectedOutputMode() !== "manual") {
          setSelectedOutputMode("manual");
        }
        btn.classList.toggle("active");
        updateProcessSelectionSummary();
        refreshPricingPreviewDebounced();
      });
    });

    setSelectedQualityMode("", false);
    setSelectedOutputMode("");
    setSelectedPreset("");
    updateProcessSelectionSummary();



    function getSelectedOutputMode() {
      const active = document.querySelector(".output-mode-btn.active");
      return active?.dataset.outputMode || "";
    }

    function setSelectedOutputMode(mode) {
      const normalized = mode === "manual" || mode === "preset" ? mode : "";
      document.querySelectorAll(".output-mode-btn").forEach((btn) => {
        btn.classList.toggle("active", normalized !== "" && btn.dataset.outputMode === normalized);
      });

      const presetPanel = document.getElementById("presetOutputsPanel");
      const manualPanel = document.getElementById("manualOutputsPanel");
      if (presetPanel) presetPanel.style.display = normalized === "preset" ? "block" : "none";
      if (manualPanel) manualPanel.style.display = normalized === "manual" ? "block" : "none";

      if (normalized === "preset" && !getSelectedPreset()) {
        setSelectedPreset("");
      }

      updateOutputsModeNote();
      updateProcessSelectionSummary();
    }

    function getSelectedPreset() {
      const active = document.querySelector("[data-preset].active");
      return active?.dataset.preset || "";
    }

    function setSelectedPreset(presetName) {
      const presets = {
        inspection_standard: ["las", "mesh_obj", "pdf_report"],
        inspection_tams: ["las"],
        web_delivery: ["cesium_tiles_pointcloud", "pdf_report"],
        compact_delivery: ["laz", "mesh_obj", "pdf_report"]
      };

      document.querySelectorAll("[data-preset]").forEach((btn) => {
        btn.classList.toggle("active", presetName !== "" && btn.dataset.preset === presetName);
      });

      const selected = new Set(presetName ? (presets[presetName] || []) : []);
      document.querySelectorAll("[data-output]").forEach((btn) => {
        btn.classList.toggle("active", selected.has(btn.dataset.output));
      });

      updateProcessSelectionSummary();
    }

    function updateOutputsModeNote() {
      const el = document.getElementById("outputsModeNote");
      if (!el) return;
      if (getSelectedOutputMode() === "manual") {
        el.textContent = "Está activada la selección manual. Puede marcar exactamente los archivos que quiera recibir.";
      } else if (getSelectedOutputMode() === "preset") {
        const presetTexts = {
          inspection_standard: "Paquetes prediseñados",
          inspection_tams: "Paquete técnico reservado para flujo interno TAMS.",
          web_delivery: "Paquete orientado a visualización web con Cesium 3D Tiles.",
          compact_delivery: "Paquete recomendado cuando se quiere una entrega más ligera."
        };
        el.textContent = presetTexts[getSelectedPreset()] || "Paquetes prediseñados";
      } else {
        el.textContent = "Seleccione cómo quiere recibir los archivos.";
      }
    }

    function formatQualityModeLabel(mode) {
      if (mode === "fast") return "Rápido";
      if (mode === "normal") return "Normal";
      if (mode === "max") return "Máxima calidad";
      return "Sin seleccionar";
    }

    function updateSelectedQualityBadge() {
      if (!selectedQualityBadge) return;
      selectedQualityBadge.textContent = `Modo seleccionado: ${formatQualityModeLabel(getSelectedQualityMode())}`;
    }


    function getSelectedPreset() {
      const active = document.querySelector("[data-preset].active");
      return active?.dataset.preset || "";
    }

    function formatPresetLabel(preset) {
      const labels = {
        inspection_standard: "Inspección estándar",
        inspection_tams: "TAMS",
        compact_delivery: "Entrega compacta"
      };
      return labels[preset] || "Sin seleccionar";
    }

    function formatDeliveryModeLabel(mode) {
      if (mode === "preset") return "Paquetes recomendados";
      if (mode === "manual") return "Selección manual";
      return "Sin seleccionar";
    }

    function updateProcessSelectionSummary() {
      const box = document.getElementById("processSelectionSummary");
      if (!box) return;

      const quality = getSelectedQualityMode();
      const deliveryMode = getSelectedOutputMode();
      const preset = getSelectedPreset();
      const outputs = getSelectedOutputs();

      const lines = [];

      if (quality) {
        lines.push(`Calidad: ${formatQualityModeLabel(quality)}`);
      }

      if (deliveryMode === "preset") {
        lines.push(`Entrega: ${formatDeliveryModeLabel(deliveryMode)}`);
        if (preset) {
          lines.push(`Paquete: ${formatPresetLabel(preset)}`);
        }
      } else if (deliveryMode === "manual") {
        lines.push(`Entrega: ${formatDeliveryModeLabel(deliveryMode)}`);
        lines.push(`Selección manual: ${outputs.length ? outputs.map(formatOutputLabel).join(", ") : "Sin seleccionar"}`);
      }

      if (!lines.length) {
        box.style.display = "none";
        box.innerHTML = "";
        return;
      }

      box.style.display = "block";
      box.innerHTML = lines.join("<br>");
    }

function getSelectedQualityMode() {
      return qualityModeInput?.value || "";
    }

    function getSelectedOutputs() {
  return Array.from(document.querySelectorAll('[data-output].output-btn.active'))
    .map((btn) => btn.dataset.output)
    .filter(Boolean);
}

    function isTamsPresetSelected() {
      return getSelectedOutputMode() === "preset" && getSelectedPreset() === "inspection_tams";
    }

    function buildXprocesMeta() {
      return {
        outputs_requested: getSelectedOutputs(),
        preset_key: getSelectedPreset() || "",
        output_mode: getSelectedOutputMode() || "",
        tams_export: isTamsPresetSelected()
      };
    }

    function formatOutputLabel(output) {
      const labels = {
        las: "LAS",
        laz: "LAZ",
        e57: "E57",
        ply: "PLY",
        pts: "PTS",

        mesh_obj: "Malla OBJ",
        mesh_fbx: "FBX",
        mesh_stl: "STL",
        mesh_dae: "DAE",
        mesh_3ds: "3DS",
        mesh_wrl: "WRL",

        mesh_ply: "Malla PLY",
        mesh_u3d: "Malla U3D",
        orthomosaic_tif: "Ortomosaico TIFF",
        orthomosaic_jpg: "Ortomosaico JPG",
        dem_tif: "DEM/DSM TIFF",
        pdf_report: "Informe PDF"
      };
      return labels[output] || output;
    }

    function formatSelectedOutputsText() {
      const outputs = getSelectedOutputs();
      if (!outputs.length) return "Sin seleccionar";
      return outputs.map(formatOutputLabel).join(", ");
    }

    function formatOutputModeLabel(mode) {
      if (mode === "manual") return "Entrega: selección manual";
      if (mode === "preset") return "Entrega: paquetes recomendados";
      return "Entrega";
    }

    function getSelectionSummaryItems() {
      const items = [];
      const quality = getSelectedQualityMode();
      items.push({ type: "text", text: `Calidad: ${formatQualityModeLabel(quality)}` });

      const outputMode = getSelectedOutputMode();
      items.push({ type: "text", text: formatOutputModeLabel(outputMode) });

      if (outputMode === "preset") {
        const presetBtn = document.querySelector('[data-preset].active');
        const presetKey = presetBtn ? String(presetBtn.dataset.preset || "").trim() : "";
        const presetLabel = presetBtn ? presetBtn.textContent.trim() : "Sin seleccionar";

        items.push({ type: "text", text: `Paquete: ${presetLabel}` });

        if (presetKey === "inspection_tams") {
          items.push({
            type: "tams",
            text: "TAMS"
          });
        }
      } else {
        const outputs = getSelectedOutputs();
        const outputsText = outputs.length
          ? outputs.map(formatOutputLabel).join(", ")
          : "Sin seleccionar";
        items.push({ type: "text", text: `Archivos de entrega: ${outputsText}` });
      }

      return items;
    }

    function renderProcessSelectionChips(items) {
      if (!processSelectionSummary) return;

      processSelectionSummary.style.display = "block";
      processSelectionSummary.innerHTML = `
        <div class="selection-summary-title">Configuración seleccionada</div>
        <div class="selection-chips">
          ${items.map((item) => {
            if (item && item.type === "tams") {
              return `<span class="selection-chip"><img class="selection-chip-logo" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAANwAAAAzCAYAAAD8SGbWAAABN2lDQ1BBZG9iZSBSR0IgKDE5OTgpAAAokZWPv0rDUBSHvxtFxaFWCOLgcCdRUGzVwYxJW4ogWKtDkq1JQ5ViEm6uf/oQjm4dXNx9AidHwUHxCXwDxamDQ4QMBYvf9J3fORzOAaNi152GUYbzWKt205Gu58vZF2aYAoBOmKV2q3UAECdxxBjf7wiA10277jTG+38yH6ZKAyNguxtlIYgK0L/SqQYxBMygn2oQD4CpTto1EE9AqZf7G1AKcv8ASsr1fBBfgNlzPR+MOcAMcl8BTB1da4Bakg7UWe9Uy6plWdLuJkEkjweZjs4zuR+HiUoT1dFRF8jvA2AxH2w3HblWtay99X/+PRHX82Vun0cIQCw9F1lBeKEuf1UYO5PrYsdwGQ7vYXpUZLs3cLcBC7dFtlqF8hY8Dn8AwMZP/fNTP8gAAAAJcEhZcwAACxMAAAsTAQCanBgAACDQSURBVHic7Z15mBTVucZ/Pd0zzMa+KwgKLoigICqKiICKRsUFjBqi3Bs0Gk2M0RgjNzEmGkNcY6LGNYmKuBAWQWWRTUA2ARc2UVAW2Ydhmxlm7/vHew5V3V1dXT3gDCa+z1PPTFedOlV16nz7950KRaNRvsN3+A61gxA/jp4AZNfS9baazQvtgUYB+igENgS8Xn3gaCAjYPsC4OsUbfKBjgH7A9gObE7RpgnQHegNNAMuM9eJx2rgfTSGU4HlAa6fDVwEXAVc69q/FBgDvAJs9DyzBM48ej7zrjkL9qunFVs6c9Ko5ZAZ4MrxyAfGXArr367Byd9+RKNRIsA04MhauuafgOEe+3OB6cAxAfpYDZwClAZoezbwbtCbA+YC/YAKnzZDgGfS6PMp4KdJjoWA/wFuBU4N0NfpZrN4BXgemJOk/RHAy0B/xKgmAJXmWHfgj8A9wCBEwAeLvkBrzyPV7CKH98jMq/Q8/l+CCLCvFq+XjEguRhIuCDoC5wMTA7StRLw5J2DfxyOiX+3T5rSAfVkke+Z2wHPABWn258Z1iAHcATwRd6wtYqbHAT9BhFfiOp4JXAmMAKYAA0k1piGoqMrUE2UBidbIK8Qz7ygQBpoDny/MZPviIM/1H4ugqtY3jSsJfi9h0/6bQHPgBJ/j+UC3Q3Cdo5DkPRhis8gA/gLcG7f/SURsZwNFwD+A2cACpJb+EHgDOAv4HKmX7WN6iEJJZa7zuww6tl7Db855APYC1UhGOxhonsmR6Fk8wL5NF/DaBT0YP+A09q6dbO7hHaAN8HPgPWCwOeOHSNreaX73NL8fN/f3N2A+kuo/MG1+ZtoMA94EXiOYeVLriNT1DaBB753mOecCLYFth/xuJMHeSnLsFLMdLEYAJx6Cftz4LVLLP0CTdCCapB8gVd6OcRSRyTlAQ0SsQ9EkvgH4je0wHK6iff11jiSrgvxwEfef91uqomH+NOsesaBMbJulpuVnwJOEgIrKGUy6cCaFy7siidvIdc8zkM3cF9nl/0Z25vlIk3kU6GV+7zXH3ar3meYZOpk2JyEbeCRQnMbY1RoOBwnXh/RtyPaIc38TOA2ol+TYWRz8mPUn1nlxqBBBBANwKZpw1tYsN3/7ofu/x/y+2PxdACwErsHKrEpo2ngnr/W/1jk7ZHoqggf7Deeucx6W7KxKuJeWB/6riuZTtB7gQURsDyMVfyRwLHJU7UaaRQ7QwZzZADHjY83vEkRsH5s76Ya0nZtMO8yztQF+hL8dXmeIIF2/ttA47ncG4sQ1wUBgLF6WxMGhB9CCRM9dGDjjEPQ/OHUT1gKTcSbNkeh5kzECi7PQfXZCE/Rzs9/2MxRJgfPMb7fNtgypaE2AnQDbS1qQQXXsFUKIwIrgof6/IhyuYsSHv07OhkJUEMkLUb6vi9kzFVmB7yH1sTEiuHrAhUiyfYAcPt9HBLgWqeGY53sEh6hPxvHozkBe4cMWEfTQDX3aZAC3oImYDNWImy5Kcb2P4353AL6X4pxkuBw5HtbV8PxkaAJ0JpHgGqEJfTDIRmqQH+aid7I+bv+ZaIy7+px7HNAFqV9uWM/gULOBpMDLrjbWGjvAwB4/6xdEsioxNHcD0ip+hnW0VcCDPYYz4uNfe9lzFhGqK6LAGkQ0ZyDVsqc5vhkR2BDgLkTONwAvIQ9uG+BZJEv7IDt7J3rv/zbtb0Jz6ZswMQ4pIsD4AO364k9wUWSsvp/m9ZPFm0ADORcNshcaIJXoqTSvaVGAJmIrj2N9kIRxo0uStpajtghwzTCKDfrhORKJDWRj3QmMItHrOhNYheyXCuAjJB1OBFbiSMZ7UDjgKhSKcSuDJyN7qtDu+NHx/yCcWWX9rJej8b4bS3BR2Fjc1kvHcPsGGpDVAEp3Ponm0QOIQNqaex2F3vO9SLrNRTbgJ8CNpo8JKOZ4ByK4AejdlSAHyWrgEsQsD2sEsUfCQF6Adi1TN4lBBH9v4xLg/4AynzYX+xxLhW3Ah3irpP1IdCj1StLPShIltx+qUxw/h+QetmnIpjkqbrsSEcIwYAXy4OXgeAttf5/gSKgnzTkgBtMDeNV9sc0lR7hHZ4f56xBpCCqrIpr2sdJtH7CMKMvIzizgvNEA41C871PkZlmCCHAKsjdfMvc+0vQx0fweZ+57n3nOaSh8cxIK+cxBTG8Z/uGcwwJ16aXsin9MazEiiA9J7iDpZ/r5tAbXz0MvshuO0W1xKnqpK1z7ktlvqwmed1GOVCi/TJUb0GQaibj6Z2hC2am/J8B1liLO/xM0cR9Bk3sPYjS3oLHbh+y9l5HK9gIgNlwM5ZVZydREoQoa5O3l3A6zmLXxXPdsWoFVfUNAdjO7f5zZvPC/cb8nkhgXfB9vLeohsx32qEsv5TX4E/xsNEEn+LSpB1xdw+vnI4L70uNYmFiJ1gBvlboc2R+pnBkWFch1nwo9kQSahZwL7yIv3yCCO7luR4T3LrJ1hwPzzLGRyIbbhFTVI5BTRilo+2FIt1c5sukmf19fJTRrXsAb510tWeOFEFB+WHro6wR1RXDZwBU+xwuRqxpkn/jhCmomqS3BrUhy3O0gOQfZDvHYhiRxMjvUC6MJJqUsuiDv3T3ISTAL2Zd34XjuvLAdxabGIe6/ARHcbLNtQ/Z7MVIpnXHeD7d2eYqmjXc67hYvZELRznwunTIxeS5POdCiHZwaH5f/70RdEdx5OPEWLyxE3Bfk2l7q0/Y4RBDpImS2ZJ7VM3DG51S8iXoTUvmC2LgWq1CQuqY4BjkNHkLj9DRylnihEDlILkDOiTNRALw3sj1vQ84SSb4QsAuGdH+Vrkd/Kl+nn0oZglU7OrFo/eleo3MEcARVNCMvD9pcmO5zBkV90mN4dYq6Irir8X+Vs3F4616kViVDGHnQ0kUUvahFOKFdN45E0gViE4bdWISTLZgO/gb8Nc1zvNAK2WlzSO6AqkLjdxOyNe3W19xHwYGWldAkv5ABHaaQFyn2Cmg7CKn9tTNf83r6vsjTuoEQH1NFc0p3JDQ6BOiFYm81NStqHXVBcA3xj2eV4qiTFjPwJgqLC6gZl8tFnN6rZCgfudUzkWPBC/PN35oE33+OiGBNDc6NxwkoF/KqFO0qXVssQsBe6HjkGq477RUpvX4s0aC8Ostr97VI5oUR47rEdawbkvAPAj/G0Q6aIq/qQ8AvcJgdKJRxq9kuM/v6IkbSA5kV/VPfbd2jLgjuCvzLcD5Gdoob0xBhJMNx1CxEYBUhrxR2mz5k3fDxKEUxo4PBc2ii3IFU04PFKORdTR8V0Kjpbu4/7beBiQ0gHEoQgy2RxLH2LcBQotWgZ52OiKY9CmjPQ/mpk1HFQz3gMeSd7oUcRdORE+lJZHf+FHlereV4MfD7oI9al6gLgkuV6f8Zkiq5ri0DeQOTIYReTE0RL1EtjkHc2ktlXELqYtUg2ICSjPuiCfYAss2S+f38EEHJxwHJxYUqOKrJBi44fqq/LmERBbIgIyMhrHgx8uouQYSxC+hDOLMtGZmXo1Sur5D39Kco1toVSaqt6F3cjPM+/47iiDcjzWgfUoWfwyHovwPXp/fAdYPajsN1JHVlwGC8pZWn7uJCXySJglaDu5GMmE8jeYZNEPd+OrDV8PNQdn8+qoo4Gade0C8Fz+I8ZNttSevq1fDKOdcpzSAIuYZhQ+FRlJXXi29vE7MrELHtoZjGnHD+xWy+/R0+eXgYeseD0buahopoP0Oq8Sjk7Jln9jVHzGcDB7I4qY8KXW1932d4h3cOO9S2hLuE1HVK+WiQ47dUk60ZydPAUmEpChHE4ygUw/JCumls6aAEufXfRBLgFLRMwtgA57bC3wOciEo455jZdGi6Nrg1mg3Xz3iZTTuOdIf92yF7C6Q2rgbaUw1kZ91ATovJyDZ7FknAo1A8sD2yk+9Edll9NFd+iTJzcky7H6Ag/Z+R99qGaoJU/x8WqE0Jl8nBqX1BcA3ikH7+NS+UIk57csD2BfjblF7IQRMrG3H/CmR7uiXoLJLHHeej8ZuMwgJ+OJGg9mUGsBueGPxz8hoVK28/oEKaFS6Pb2urDXahOrsQ0JoIN1LESez85AXkvXwU+AKNeX8kyU9BOaAXIKfJY4ipVSCp/RUisgzEtN9DuZad0HsvIBhDqlPUJsF15tCUt/jhfPQCgiyuE4+FabSdhylhSQMRxJ3bIYZQRWIdYF9SB/qfQ8/pp50ESzULAXvgutNfoU3rr6WspWH9lVXVc0vEfJwK7DnAHw4ciXIZUVrRvMdAvng1G6J/QBlERyHnx2sol/Ju5LltYI7fjCT2CyjQb/EiGs8XEbH2RXfyHcG5cBk1W+spHWQiVaQmBPcpwdc/WUT6BY77kAfOK2PF4kyU4zjDp83JpCaL1PZMCD1BOVxy3Ns0a1ggFhKU4KLQOncL4XAVVdEwhCjF0WBimVE1p5NFDo06F0K0EjmjmqBUMqspdEc2vrXNluru2IIknB23cpySrFE4iQvxJUmHJWqL4OohsV8buBwtYZAuNiI7rmeqhshlXRPMJDa+FI8I8sDdgbLo43ExikX5kcU+nMLT5KiCvGgxdw54lEHHj5GbIh3f5n54pd91LNjWk/WF7SCLSp/rbqQSyD0QKvXKHKpAWTirPI7tMls8qn2ueViitpwmfZC9UhvoTuoiTy9YrpoKm5H9URNMCtDmRGSnvYxSr24y27NorZWmKc6fQbJ1Jt0og05HruL3fX5HeH+6Ji9QDZkNKsjKLA/maNkPtO0J5z4PWUGcrf+ZqC0JN5jUxP1HgkwU2S9+zpdMFJOZT/rxqIWodMUPnxDsPr0wGcWfglS5X2e2dPE8wSJp7Chp7l9t6Ids+Ov829ha2CqYoRBCsrfHDfDJ41CeTv72fw5qg+BaknqCbcS1WlQKzCW1t/Mi5A1Mty5kKVKu/CqHP8Q/hz4VHqDmy0qkwpto+Tl/VENefjGv9huSuhw2GTLh+VU3sm9ffQVsorRB+aEt0Pjcg5jeqWjhoCxClFLOzUSrD0U627cBDdC8/hjZm7VCcOeTelWuWZ57o+gOc5DjvhzIYAtyER/t0187pFZuJj0ptxY5HPwI7qM0+vPCfDQZ/3SQ/cTjI5TInBpRKK7Io1erD2q+BFMUjsjbzPLwSbaPfMRIbG3gAPSsvZEXUfk6uzdlUbUfFCKxyQw7kI3WDMeZsh/FE0twMnqyiU2z+xKH+bUz116HspNamH6KUHaLdbpsRQ6WbHNOJXrvGThLJGWi+VWKk0iRae45ZPrcbM45zvSxBtUq5iC7silaC3QgYoSzga8PrQ1XjVJRnXUKQwRbtHWW595mpp8VE/RIWjWkMGn7WFgpF0i9MtiPv4dzK+ktp5AMI5CkO1SYhBKXC1M1BCAKF7WbREk091CueVaJswxDOU580TqhisilhJnDsti3/k5kB68223QUh/uF+f0qisGtRozkDPT2x7nOWY3CBfVQLG+l2TcDLXC7GuXtdjL923OmoBDVCShDZQ0KS0xBhbnDEaNYjWole5trv44Iye7/vnm2VeZZnkPEuRpJtb/grEj3fUxcNCjB+bmyIUqYCHm0BFaMh6Kdls/1wr/Q1MKpSatGZFIfWPAkTL0KplwGUwbC/MdF0DnMCTBRbkEhgmS1atl4f8TELx63Fe9VwvzWc4lfGtDit8ijejApYkuRVPse4tKpkQEUwdNn3UJu/ZKaq5TJewep3R3Q0hXHoIm4DigiI3ILir1FUGxtNEoSfwQnVasfkl5LENu9CSUtX4hTx7cBrUB2O1qAKBcxns04K1rno3heN3OdZxGRvGzuy4YvjjDXuQb5EjYjxpuPvML/RILjE+B+JJlfQUzOlje1w4kD3ovm9Mfm9wJMcnUkgFJZbQbmCNfvKsTRNO2zifL1Z4uZPhxWj4OWZ8LgeRBhPRVcQ8hXyuzEVl1HEHksHgnrxsJXruUv1k/U9uVMaH/Za5w+bDPFhJA8jX+KCqTAfEUVi4h6lu5UY5NfI+ZJ5Kx7Aw24V8Ly+gNqrqkHM7iZ5EzJz155C02SnqiItgdSSZItgb4Jccq9wNsoBzR4AN6U4Nza8ykaNd1ds/Rof4TNvY1DUvxmxNTeQrG0fAhNRmuV9EQMw6bjudX4hcjxdS+yAfOB+5BN1A5J0nrojZ2Ls0T7jxBTXIsI6ngU29uEHFBlKDjfHUmfIpS10gepwJPReA5ERP4MMomaoLc9HDm9Oph+BqPcnGZomb8GiDAz0Tw6AUnudxDREqEoZWFglFwWsvItWPa4d4swUFxQj/3bw0AV2+bDxMvh0n9vpH7kDUrM0HhZU1EzdGGgvBA+mQIzryeJrtOU9RMzWD+xhJWPTTkw4VufCwPug3d/BdtdBdzdfwPdr5V3zIuTR9F02FsIGZnQqD6UsYsKJiTcq9ue3FMKFfug4QEvn3calS1N9VvxpJRyogeWPcgyV2mH92jtQROq6kD/duyCoBpysmDQsWNoVH93+rG31MhAzG4aIueLzN1NQGpfFVLP+iMimokjIdxM2U5KyyhLkWp4CVID56OxaI4IwfpJrYTciQgu3/RbHzkt9iMVsIm51wycNEBrT9oAup1dlhFkoNzOoYhIVyKCtUsnlhLrV2iGk/97IPE+wqj2pEQIqCgBqUeDkEPiEmKLPouR/jwemMGGt7Yz6Ro48y5ofIZu0cu3lwls2QEFK2DG1VDiuXBufcRxhmOn746VoMF8lF0rH2Dtv6CsBMStfgi8xPQfzCWcCd0Ha1gtDVejYa0PFGyC0SdDTksY8A9ocIaGqgwRqq0LzzbnbF0Is4bD9gVwxSLo2Dl5zknE9LE+iZYaCkG70/U6ioEw5WRRjjv4W2yeMpvEHJhMYONWzFLiqVEMt39vLH17zpHClm6dejDUQ17neUhqzEF2TQYipD8gQpmLJqlNBCjHGcksHCcGZv+fTd/T0CQ/1hxfhhwUrZGqWYSzGtwqHA1oLlJDi00/WUgltNewWpJlzZZIliDtrjXOitjFOI6Zx0y7bBJNNNtHByRp10SoLCEgjkV6a7J8yFxUeHg1GoSr+HLMar4cA13vhFYnesd8coBFT0Bh0pXu6qF6pyFJrqmE47KS3oj7DMNZCVo2YPGTkJXjDGV2M+g5EDZugPF9oXSnttd7QscfQMf+QCs45XualJ/OhrI1sOkjWPGkc/WJfaD3Q8ndMvWALcuTawYAZzwMeU3gqEFQWgBb4ooQOv/IFB2VwvJRscdygAUPw+7gtatLCuHL0+CYJsR+vOrgkYmjVu9GEmAAsnuK0ISLIFX4ErT04RycRZy64KSk2W/MNXP9noPU0hGI9W1HtnMYOaDeQfbVFkQYHcx93IHqDS1hgHIwZxNrw1vhYaWUlU4tUHX+E2gJwxvM/rlIQls2GI7rrwgJoKuQ+tkLaB80LHAkMu6DLtHWBRmQfYHtfPpozVaOFE7E/+MXy5F+/k8cnh2bBvTBT0lA4a2wagoUxaUdrhmlDWDzrerx46fB69PMpTvhvWFBniE5Fpqc3OZ/h/3boSiunG/deGh1FKz/GLb61eAGw9QlMP89OOYOHLlTE4RgV1ljZ4nzKJsRs8tBe59GE38GIpD/RTrFm8iGOg45UubgfG9hM3K4rEW6xfPIzv4MEfAV5mqTEDvrZ55iGiLgVqatXVpxFyKuFTiZTtvQ6mcNkES0etcHqCDWRuTfRvqJbb8S2YsWL5rnLED60x5zbTvZtiAvahWSdJvNsAXCvXiXsNubTUa4v0Du0YPBOSTWns1DBnkGckqMIbZkZSfOd8++QxyObwcz/wWt25DOgn3/RGv9NwcKCMOygi6cNWYeRWX5/tkm1pZtDLx+Zawz7NBgFCLmciRJu6IJ3wvFbA8L2E8Op0KYxKpny8HuQ5zmNJRNcGxcu4F4E1wESUu3FbGN2K+xZiPd2SsHcwXigmVI5MeLnwgysqtNn7mua1WQuHZ/Q2K9jGUkpm/lE/ttgUzTj1XMGiIVKJ6J7ca9MpaQhZ7NPf4Fpm0rc60oUpvcY1IPqVHu6nfLhQNj9XpYsAguawMZIWoWi6uCLkcu46KjJzH6sxRrF4XRYrDbtkD57hpczBeZiCGPQONSaX7/ksOI2CyCSLgMJF4vcu0rR+L4Wde+zkgK5rr2LSRWMoaRu3cAcr64J9xcpLaOQpLpZBS/8IqVWXyCXL9+bbagSWyftRTHVWzxGrHVDHuRKutucx/wO9fvPcgd3co8Uz+87dtVSMV5Eyc5+kRkQ7gTkW9DqtIMHNX9EZw6sOvRmPUj1lm1DhVjTiL5MuIJqJ8LexfjlMKmRqyEA4jA2C+uZNCUMc4qn17IB5a/BVMvD3p7/5EIKuGqkafGTXBZ6Ks1fZC0KUATZXDC2Q5aI5082epaZ5vtZuT8+AK5cf2IqZrUodtcYqdCNsoeeN38bkHiQrINkHT+u2tf/Mc8xiEpOoHkC7Fi2nRCXtbLEKF53ff5KIjttpO3I5voJZIvgdceVT7fiDI0biJADmlZBTz9EtxyPTX/dGEWDJvzos6v597LlTjOh+3AWEIhr/KaoBiKMkG8ljP8ViGoyfws3h8ovBZ5iJ5BdtUC5BHqHtc2D0muIEvZtUTBxatITUw5xEpUL6wkcb0Sd/lOf5ygvhvue21O4qeGFyJC8CM2NxrhqNcVJD7bpcSuf7kdjdlfSb3epMUQ5D5PqbmUV8C9z1DzkuBsePCD4VQUZ7r7yMP5xvaDZnsBmAIhv2ycZGiLnCr/InnGzrcKQQnua+Tu96skboZUqtuRiuMuAr2NWA8PSL0agAzc5z3u6zqUjeDlBnzGnHcpUj2nxh3fg1TErsglOzLuuLvINNn6IL1wvnLTm9hMiLXIdmsUd04BIo5O5hqr4453QZLer9pgt7n/15G6fIPrWJVrfyvTX/xS7bcScG2Wti2peS5lBN5YezXFxXluS/wys+1H7/ssMlhKGZ0oWPoCYn5PmLYN0TxZirSJp9B7XITUY7se5dmm/SyU2tUcMeRNSJ2+GzGYe5Av/G9oDt6Jo+rPRt7s+znUof40kU61wHw0Ca9Hk7gtybPqm6CBKEG5ad+PO16CJoZdD/InaJKe7WrTGQ2oVy7kOhTrs4iPllcid7LNL3wbMQA" alt="TAMS"></span>`;
            }
            const text = item && item.text ? item.text : String(item ?? "");
            return `<span class="selection-chip">${text}</span>`;
          }).join("")}
        </div>
      `;
    }

    function updateProcessSelectionSummary() {
      if (!processSelectionSummary) return;
      renderProcessSelectionChips(getSelectionSummaryItems());
    }

    function setSelectedQualityMode(mode, refresh = true) {
      const normalized =
        mode === "fast" || mode === "normal" || mode === "max" ? mode : "";

      if (qualityModeInput) {
        qualityModeInput.value = normalized;
      }

      document.querySelectorAll(".quality-mode-btn").forEach((btn) => {
        btn.classList.toggle("active", normalized !== "" && btn.dataset.qualityMode === normalized);
      });

      updateSelectedQualityBadge();
      updateProcessSelectionSummary();

      if (refresh) {
        refreshPricingPreviewDebounced();
      }
    }


    function getStoredJobs() {
      return [];
    }

    function clearStoredJobs() {
      try {
        localStorage.removeItem("xproces_jobs");
      } catch (_) {}
    }

    function formatStoredJobDate(ts) {
      try {
        return new Date(ts).toLocaleString("es-ES");
      } catch (_) {
        return "";
      }
    }

    function formatJobListStatus(status) {
      const value = String(status || "").toLowerCase();
      if (value === "queued") return "En cola";
      if (value === "running") return "Procesando";
      if (value === "done" || value === "completed") return "Completado";
      if (value === "failed") return "Fallido";
      if (value === "cancelled") return "Cancelado";
      if (value === "created") return "Creado";
      if (value === "receiving") return "Recibiendo fotos";
      return value || "Sin estado";
    }

    function formatJobListStage(stage) {
      return String(stage || "").trim() || "Sin fase";
    }

    function formatJobListPrice(price) {
      const num = Number(price);
      if (!Number.isFinite(num) || num <= 0) return "—";
      return `${num} €`;
    }

    function saveSelectedHistoryJobId(jobId) {
      try {
        if (jobId) localStorage.setItem("xproces_selected_job_id", String(jobId));
      } catch (_) {}
    }

    function getSavedSelectedHistoryJobId() {
      try {
        return localStorage.getItem("xproces_selected_job_id") || "";
      } catch (_) {
        return "";
      }
    }

    function clearSavedSelectedHistoryJobId() {
      try {
        localStorage.removeItem("xproces_selected_job_id");
      } catch (_) {}
    }

    function getSelectionSummaryItemsForJob(job) {
      const items = [];
      const quality = String(job?.quality_mode || "").trim().toLowerCase() || getSelectedQualityMode() || "normal";
      items.push({ type: "text", text: `Calidad: ${formatQualityModeLabel(quality)}` });

      const xprocesMeta = job?.exif_summary?._xproces || {};
      const requestedRaw = Array.isArray(job?.outputs_requested)
        ? job.outputs_requested
        : Array.isArray(xprocesMeta?.outputs_requested)
        ? xprocesMeta.outputs_requested
        : typeof job?.outputs_requested === "string"
        ? job.outputs_requested.split(",")
        : typeof xprocesMeta?.outputs_requested === "string"
        ? xprocesMeta.outputs_requested.split(",")
        : [];

      const normalizedRequested = requestedRaw
        .map((v) => String(v || "").trim().toLowerCase())
        .filter(Boolean);

      const explicitPresetKey = String(
        xprocesMeta?.preset_key || job?.preset_key || ""
      ).trim().toLowerCase();

      const explicitOutputMode = String(
        xprocesMeta?.output_mode || job?.output_mode || ""
      ).trim().toLowerCase();

      const explicitTams =
        xprocesMeta?.tams_export === true ||
        xprocesMeta?.tams_export === "true" ||
        xprocesMeta?.tams_export === 1 ||
        explicitPresetKey === "inspection_tams";

      const presetOutputsMap = {
        inspection_standard: ["las", "mesh_obj", "pdf_report"],
        inspection_tams: ["las"],
        compact_delivery: ["laz", "mesh_obj", "pdf_report"]
      };

      let matchedPreset = explicitPresetKey;
      if (!matchedPreset) {
        for (const [presetKey, presetOutputs] of Object.entries(presetOutputsMap)) {
          if (
            normalizedRequested.length === presetOutputs.length &&
            normalizedRequested.every((item) => presetOutputs.includes(item))
          ) {
            matchedPreset = presetKey;
            break;
          }
        }
      }

      const treatAsPreset =
        explicitOutputMode === "preset" ||
        !!matchedPreset ||
        explicitTams;

      if (treatAsPreset) {
        const presetToShow = matchedPreset || (explicitTams ? "inspection_tams" : "");
        items.push({ type: "text", text: formatOutputModeLabel("preset") });
        items.push({ type: "text", text: `Paquete: ${formatPresetLabel(presetToShow)}` });

        if (presetToShow === "inspection_tams" || explicitTams) {
          items.push({ type: "tams", text: "TAMS" });
        }
      } else {
        const outputsText = normalizedRequested.length
          ? normalizedRequested.map(formatOutputLabel).join(", ")
          : "Sin seleccionar";
        items.push({ type: "text", text: formatOutputModeLabel("manual") });
        items.push({ type: "text", text: `Archivos de entrega: ${outputsText}` });
      }

      return items;
    }


    async function fetchFullJob(jobOrId) {
      const jobId = typeof jobOrId === "string" ? jobOrId : jobOrId?.id;
      if (!jobId) return null;

      try {
        const response = await fetch(`${API_BASE}/jobs/${jobId}`);
        const data = await response.json().catch(() => ({}));
        if (!response.ok) return null;
        return data?.job || data || null;
      } catch (_) {
        return null;
      }
    }

    async function openJobFromHistory(job) {
      if (!job || !job.id) return;

      const fullJob = (await fetchFullJob(job)) || job;
      job = { ...job, ...fullJob };

      pendingProjectName = job.project_name || "";
      selectedHistoryJobId = job.id || "";
      saveSelectedHistoryJobId(selectedHistoryJobId);
      renderProcessSelectionChips(getSelectionSummaryItemsForJob(job));
      if (uploadPanel) uploadPanel.style.display = "none";
      if (datasetPanel) datasetPanel.style.display = "none";
      if (pricingPanel) pricingPanel.style.display = "none";
      if (mapWrap) mapWrap.style.display = "none";
      if (processPanel) processPanel.style.display = "block";
      if (processStartWrap) processStartWrap.style.display = "flex";

      if (uploadBtn) {
        uploadBtn.textContent = "Iniciar nuevo proyecto";
        uploadBtn.disabled = false;
      }

      if (projectTitle) {
        projectTitle.style.display = "block";
        const priceText = formatJobListPrice(job.price);
        projectTitle.textContent = "Proyecto: " + (job.project_name || "Sin nombre") + (priceText !== "—" ? ` · Precio: ${priceText}` : "");
      }

      if (jobStatusEl) {
        jobStatusEl.textContent = "";
      }

      const statusNow = String(job.status || "").toLowerCase();

if (statusNow === "done" || statusNow === "completed") {
  getJobLog(job.id)
    .then((logText) => {
      jobStatusEl.innerHTML = renderProcessSummary(job, logText || "");
      showDownloadButton(job.id);
    })
    .catch(() => {
      jobStatusEl.innerHTML = renderProcessSummary(job, "");
      showDownloadButton(job.id);
    });
}
      
      if (progressWrap) progressWrap.style.display = "block";
      if (progressFill) {
        const pct = Math.max(0, Math.min(100, Number(job.progress || 0)));
        progressFill.classList.remove("error");
        progressFill.style.background = "";
        progressFill.style.width = `${pct}%`;
        progressFill.classList.toggle("error", ["failed", "cancelled"].includes(String(job.status || "").toLowerCase()));
      }
      if (progressPercent) {
        progressPercent.textContent = `${Math.max(0, Math.min(100, Number(job.progress || 0)))}%`;
      }
      if (progressLabel) {
        const s = String(job.status || "").toLowerCase();
        if (s === "failed") progressLabel.textContent = "Fallido";
        else if (s === "cancelled") progressLabel.textContent = "Cancelado";
        else if (s === "completed" || s === "done") progressLabel.textContent = "Procesado completado";
        else if (s === "queued") progressLabel.textContent = "En cola";
        else progressLabel.textContent = "Procesando";
      }

      stopPolling();
      startPolling(job.id);
      window.scrollTo({ top: 0, behavior: "smooth" });
    }

    async function renderStoredJobs() {
      if (!jobsPanel || !jobsList || !jobsEmpty) return;

      const user = typeof getStoredUser === "function" ? getStoredUser() : null;
      if (!user || !user.id) {
        jobsPanel.classList.add("jobs-panel-hidden");
        jobsList.replaceChildren();
        jobsEmpty.style.display = "block";
        jobsEmpty.textContent = "Inicie sesión para ver sus trabajos.";
        return;
      }

      jobsPanel.classList.remove("jobs-panel-hidden");

      const hasExistingCards = jobsList.children.length > 0;
      if (!hasExistingCards) {
        jobsEmpty.style.display = "block";
        jobsEmpty.textContent = "Cargando trabajos...";
      }

      try {
        const response = await fetch(`${API_BASE}/jobs/mine`, {
          headers: {
            "x-user-id": String(user.id)
          }
        });

        const data = await response.json().catch(() => ({}));

        if (!response.ok || !data.ok) {
          throw new Error(data.message || data.error || "No se pudieron cargar los trabajos");
        }

        const jobs = Array.isArray(data.jobs) ? data.jobs : [];

        if (!jobs.length) {
          jobsList.replaceChildren();
          jobsEmpty.style.display = "block";
          jobsEmpty.textContent = "Todavía no hay trabajos en esta cuenta.";
          return;
        }

        const fragment = document.createDocumentFragment();

        jobs.forEach((job) => {
          const card = document.createElement("div");
          card.className = "job-card";
          if (selectedHistoryJobId && job.id === selectedHistoryJobId) {
            card.classList.add("active");
          }

          const statusLabel = formatJobListStatus(job.status);
          const stageLabel = formatJobListStage(job.stage);
          const progressValue = Math.max(0, Math.min(100, Number(job.progress || 0)));
          const statusLower = String(job.status || "").toLowerCase();
          const canDownload = ["done", "completed"].includes(statusLower);
          const statusBadgeClass =
            ["done", "completed"].includes(statusLower) ? "ok" :
            ["failed", "cancelled"].includes(statusLower) ? "error" :
            ["queued", "running", "receiving", "created"].includes(statusLower) ? "progress" : "";

          card.innerHTML = `
            <div class="job-card-top">
              <div class="job-card-title">${job.project_name || "Proyecto sin nombre"}</div>
              <div class="job-card-status-badge ${statusBadgeClass}">${statusLabel}</div>
            </div>
            <div class="job-card-id">Job ID: ${job.id || "—"}</div>
            <div class="job-card-meta">Estado: ${statusLabel}</div>
            <div class="job-card-meta">Fase: ${stageLabel}</div>
            <div class="job-card-meta">Progreso: ${progressValue}%</div>
            <div class="job-card-meta">Precio: ${formatJobListPrice(job.price)}</div>
            <div class="job-card-meta">Creado: ${formatStoredJobDate(job.created_at)}</div>
            ${canDownload ? `<div class="job-card-meta" style="margin-top:8px;"><a class="process-download-btn" href="${API_BASE}/jobs/${job.id}/download" target="_blank" style="min-height:36px; padding:8px 12px; font-size:13px;">Descargar resultados</a></div>` : ""}
          `;

          card.addEventListener("click", async (event) => {
            if (event.target.closest("a")) return;
            selectedHistoryJobId = job.id || "";
            saveSelectedHistoryJobId(selectedHistoryJobId);
            await openJobFromHistory(job);
            await renderStoredJobs();
          });

          fragment.appendChild(card);
        });

        jobsList.replaceChildren(fragment);
        jobsEmpty.style.display = "none";
      } catch (error) {
        console.error("renderStoredJobs error", error);
        if (!jobsList.children.length) {
          jobsEmpty.style.display = "block";
          jobsEmpty.textContent = "No se pudieron cargar los trabajos.";
        }
      }
    }

    function getStoredUser() {
      try {
        const raw = localStorage.getItem("xproces_user");
        return raw ? JSON.parse(raw) : null;
      } catch (_) {
        return null;
      }
    }

    function setStoredUser(user) {
      localStorage.setItem("xproces_user", JSON.stringify(user));
    }

    function clearStoredUser() {
      localStorage.removeItem("xproces_user");
    }

    function openAuthModal(mode = "login") {
      authMode = mode;

      authTitle.textContent =
        mode === "register" ? "Registrarse" :
        mode === "invite" ? "Código invitado" :
        "Iniciar sesión";

      authSubtitle.textContent =
        mode === "register"
          ? "Cree su cuenta para asociar sus trabajos."
          : mode === "invite"
          ? "Acceda con un código de un solo uso."
          : "Introduzca sus credenciales para continuar.";

      const user = getStoredUser();
      authEmail.value = user?.email || "";
      authName.value = user?.name || "";
      authPassword.value = "";
      authCode.value = "";

      if (authNameWrap) {
        authNameWrap.style.display = mode === "register" ? "block" : "none";
      }

      authPasswordWrap.style.display = mode === "invite" ? "none" : "block";
      authCodeWrap.style.display = mode === "invite" ? "block" : "none";

      authBackdrop.classList.add("show");
    }

    function closeAuthModal() {
      authBackdrop.classList.remove("show");
      if (modalState.restoreAuthAfterClose) {
        modalState.restoreAuthAfterClose = false;
      }
    }

    function renderTopbarUser() {
      const user = getStoredUser();

      if (!user || !user.email) {
        topbarActions.innerHTML = `
          <button class="btn btn-secondary" id="loginBtn" type="button">Iniciar sesión</button>
          <button class="btn btn-primary" id="registerBtn" type="button">Registrarse</button>
          <button class="btn btn-secondary" id="inviteBtn" type="button">Código invitado</button>
        `;

        document.getElementById("loginBtn").addEventListener("click", () => openAuthModal("login"));
        document.getElementById("registerBtn").addEventListener("click", () => openAuthModal("register"));
        document.getElementById("inviteBtn").addEventListener("click", () => openAuthModal("invite"));
        return;
      }

      const userType = user.auth_type === "invite" ? "Invitado" : "Usuario";

      topbarActions.innerHTML = `
        <div class="user-pill">${userType} · ${user.name || "Usuario"} · ${user.email}</div>
        <button class="btn btn-secondary" id="logoutBtn" type="button">Salir</button>
      `;

      document.getElementById("logoutBtn").addEventListener("click", () => {
        clearStoredUser();
        clearStoredJobs();
        renderTopbarUser();
        renderStoredJobs();
      });
    }

    function formatBytes(bytes) {
      const mb = bytes / 1024 / 1024;
      const gb = mb / 1024;
      if (gb >= 1) return gb.toFixed(2) + " GB";
      return mb.toFixed(2) + " MB";
    }

    function formatEtaText(seconds) {
      const total = Math.max(0, Number(seconds || 0));
      const hours = Math.floor(total / 3600);
      const minutes = Math.ceil((total % 3600) / 60);
      if (hours <= 0) return `${minutes} min`;
      return `${hours} h ${minutes} min`;
    }

    function chunkArray(arr, size) {
      const chunks = [];
      for (let i = 0; i < arr.length; i += size) {
        chunks.push(arr.slice(i, i + size));
      }
      return chunks;
    }

    function stopPolling() {
      if (pollTimer) {
        clearInterval(pollTimer);
        pollTimer = null;
      }
    }

    function enableUploadButton() {
      uploadBtn.disabled = false;
    }

    function disableUploadButton() {
      uploadBtn.disabled = true;
    }

    function hideDownloadButton() {
      processDownloadWrap.style.display = "none";
      processDownloadBtn.href = "#";
    }

    function showDownloadButton(jobId) {
      processDownloadBtn.href = `${API_BASE}/jobs/${jobId}/download`;
      processDownloadWrap.style.display = "block";
    }

    function resetDetectionInfo() {
      fieldCamera.textContent = "Sin detectar";
      fieldDrone.textContent = "Sin detectar";
      fieldLocation.textContent = "Sin detectar";
      fieldLens.textContent = "Sin detectar";
      fieldFocal.textContent = "Sin detectar";
      fieldDate.textContent = "Sin detectar";
      if (files.length > 0) mapWrap.style.display = "block";

      if (map && marker) {
        map.removeLayer(marker);
        marker = null;
      }
    }

    function resetPreviewState() {
      if (previewTimer) {
        clearTimeout(previewTimer);
        previewTimer = null;
      }
      previewRequestId++;
      lastPreview = null;
      priceBig.textContent = "0 €";
      etaBig.textContent = "Sin calcular";
    }

    let modalState = {
      restoreAuthAfterClose: false
    };

    function closeModal(result = false) {
      const restoreAuthAfterClose = modalState.restoreAuthAfterClose;

      modalBackdrop.classList.remove("show");
      modalBox.className = "modal";
      modalConfirmBtn.disabled = false;
      modalCancelBtn.disabled = false;

      const resolver = modalResolver;
      modalResolver = null;
      modalState.restoreAuthAfterClose = false;

      if (resolver) {
        resolver(result);
      }

      if (restoreAuthAfterClose) {
        authBackdrop.classList.add("show");
      }
    }

    function openModal({
      title = "Mensaje",
      subtitle = "",
      body = "",
      confirmText = "Aceptar",
      cancelText = "Cancelar",
      showCancel = true,
      tone = "default",
      hideAuthWhileOpen = false
    }) {
      return new Promise((resolve) => {
        if (modalResolver) {
          const previousResolver = modalResolver;
          modalResolver = null;
          previousResolver(false);
        }

        const shouldHideAuth = hideAuthWhileOpen && authBackdrop.classList.contains("show");
        if (shouldHideAuth) {
          authBackdrop.classList.remove("show");
        }

        modalState.restoreAuthAfterClose = shouldHideAuth;
        modalResolver = resolve;

        modalTitle.textContent = title;
        modalSubtitle.textContent = subtitle;
        modalBody.innerHTML = body;
        modalConfirmBtn.textContent = confirmText;
        modalCancelBtn.textContent = cancelText;
        modalCancelBtn.style.display = showCancel ? "inline-block" : "none";
        modalConfirmBtn.disabled = false;
        modalCancelBtn.disabled = false;

        modalBox.className = "modal";
        if (tone === "warning") modalBox.classList.add("modal-warning");
        if (tone === "error") modalBox.classList.add("modal-error");

        modalBackdrop.classList.add("show");
      });
    }

    async function showInfoModal(title, subtitle, message, tone = "default", options = {}) {
      await openModal({
        title,
        subtitle,
        body: `<div class="modal-note">${message}</div>`,
        confirmText: "Entendido",
        showCancel: false,
        tone,
        hideAuthWhileOpen: options.hideAuthWhileOpen ?? authBackdrop.classList.contains("show")
      });
    }

    modalCancelBtn.addEventListener("click", () => closeModal(false));
    modalConfirmBtn.addEventListener("click", () => closeModal(true));

    authCancelBtn.addEventListener("click", closeAuthModal);

    authSaveBtn.addEventListener("click", async () => {
      const email = authEmail.value.trim();
      const name = authName.value.trim();
      const password = authPassword.value;
      const code = authCode.value.trim().toUpperCase();

      if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
        await showInfoModal(
          "Email no válido",
          "Introduzca un correo correcto.",
          "Ejemplo: cliente@empresa.com",
          "warning"
        );
        return;
      }

      if (authMode === "register" && !name) {
        await showInfoModal(
          "Falta el nombre o empresa",
          "Debe identificar a la persona o empresa.",
          "Introduzca un nombre o empresa para continuar.",
          "warning"
        );
        return;
      }

      if (authMode === "invite") {
        if (!code) {
          await showInfoModal(
            "Falta el código",
            "Debe introducir un código invitado.",
            "Introduzca un código válido para continuar.",
            "warning",
            { hideAuthWhileOpen: true }
          );
          return;
        }
      } else if (!password || password.length < 6) {
        await showInfoModal(
          "Contraseña no válida",
          "La contraseña debe tener al menos 6 caracteres.",
          "Introduzca una contraseña válida para continuar.",
          "warning"
        );
        return;
      }

      try {
        let endpoint = "";
        let payload = {};

        if (authMode === "register") {
          endpoint = `${API_BASE}/auth/register`;
          payload = { email, password, name };
        } else if (authMode === "invite") {
          endpoint = `${API_BASE}/auth/invite-login`;
          payload = { email, code };
        } else {
          endpoint = `${API_BASE}/auth/login`;
          payload = { email, password };
        }

        const response = await fetch(endpoint, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        });

        const data = await response.json().catch(() => ({}));

        if (!response.ok || !data.ok) {
          throw new Error(data.message || data.error || "Error de acceso");
        }

        setStoredUser(data.user);
        renderTopbarUser();
        renderStoredJobs();
        closeAuthModal();
      } catch (error) {
        console.error(error);
        await showInfoModal(
          "No se pudo completar el acceso",
          "Revise sus datos e inténtelo de nuevo.",
          error.message || "Error inesperado",
          "error"
        );
      }
    });

    modalBackdrop.addEventListener("click", (e) => {
      if (e.target === modalBackdrop) {
        closeModal(false);
      }
    });

    authBackdrop.addEventListener("click", (e) => {
      if (e.target === authBackdrop) {
        closeAuthModal();
      }
    });

    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && modalBackdrop.classList.contains("show")) {
        closeModal(false);
      }

      if (e.key === "Escape" && authBackdrop.classList.contains("show")) {
        closeAuthModal();
      }
    });

    function refreshMapAfterResize() {
      if (!map) return;
      const mapEl = document.getElementById("map");
      if (!mapEl) return;
      if (mapEl.offsetWidth === 0 || mapEl.offsetHeight === 0) return;

      if (mapResizeTimer) {
        clearTimeout(mapResizeTimer);
      }

      mapResizeTimer = setTimeout(() => {
        if (!map) return;
        map.invalidateSize(true);
        mapResizeTimer = null;
      }, 180);
    }

    window.addEventListener("resize", refreshMapAfterResize);
    window.addEventListener("orientationchange", () => {
      setTimeout(refreshMapAfterResize, 220);
    });

    async function fetchPricingPreview(photoCount, totalBytes) {
      try {
        const response = await fetch(`${API_BASE}/pricing/preview`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            photos_count: photoCount,
            total_bytes: totalBytes,
            quality_mode: getSelectedQualityMode(),
            outputs_requested: getSelectedOutputs(),
            tams_export: isTamsPresetSelected()
          })
        });

        if (!response.ok) return null;
        return await response.json();
      } catch (_) {
        return null;
      }
    }

    function refreshPricingPreviewDebounced() {
      if (previewTimer) {
        clearTimeout(previewTimer);
        previewTimer = null;
      }

      if (files.length === 0) {
        lastPreview = null;
        priceBig.textContent = "0 €";
        etaBig.textContent = "Sin calcular";
        return;
      }

      const requestId = ++previewRequestId;
      jobStatusEl.textContent = "Calculando precio y tiempo...";

      previewTimer = setTimeout(async () => {
        try {
          const preview = await fetchPricingPreview(files.length, totalSize);

          if (requestId !== previewRequestId) return;

          if (preview && preview.ok) {
            lastPreview = preview;
            priceBig.textContent = `${preview.price} €`;
            etaBig.textContent = preview.estimated_human || "No disponible";
            jobStatusEl.textContent = "Pendiente de inicio. El trabajo todavía no se ha enviado a procesado.";
          } else {
            lastPreview = null;
            priceBig.textContent = "No disponible";
            etaBig.textContent = "No disponible";
            jobStatusEl.textContent = "No se pudo obtener el precio y el tiempo.";
          }
        } catch (_) {
          if (requestId !== previewRequestId) return;
          lastPreview = null;
          priceBig.textContent = "No disponible";
          etaBig.textContent = "No disponible";
          jobStatusEl.textContent = "No se pudo obtener el precio y el tiempo.";
        }
      }, PREVIEW_DEBOUNCE_MS);
    }

    function setDatasetHighlight(text, type = "ok") {
      datasetHighlight.style.display = "flex";
      datasetHighlightBadge.textContent = text;
      datasetHighlightBadge.className = "dataset-highlight-badge";

      if (type === "warn") datasetHighlightBadge.classList.add("warn");
      if (type === "bad") datasetHighlightBadge.classList.add("bad");
    }

    function hideDatasetHighlight() {
      datasetHighlight.style.display = "none";
      datasetHighlightBadge.textContent = "";
      datasetHighlightBadge.className = "dataset-highlight-badge";
    }

    function fillDatasetGrid(dataset, missionType) {
      fieldSize.textContent = formatBytes(totalSize);
      fieldResolution.textContent = dataset.megapixels > 0 ? `${dataset.megapixels} MP` : "Sin detectar";
      fieldGps.textContent = dataset.gpsDetected ? "Detectado" : "No detectado";

      if (dataset.cameras.length === 1) {
        fieldCamera.textContent = dataset.cameras[0];
      } else if (dataset.cameras.length > 1) {
        fieldCamera.textContent = "Múltiples cámaras";
      } else {
        fieldCamera.textContent = "Sin detectar";
      }

      fieldMission.textContent = missionType || "Sin detectar";
      fieldOverlap.textContent = dataset.overlap?.message || "Sin detectar";
    }

    function setAnalysisPlaceholder() {
      analysisBadgeEl.textContent = "Próximamente";
      statusEl.textContent = "";
    }

    function resetStageClasses() {
      processStageWrap.querySelectorAll(".process-stage").forEach((el) => {
        el.classList.remove("active", "done", "error");
      });
    }

    function mapJobToStage(job) {
      const stage = String(job.stage || "").toLowerCase();

      if (
        stage === "matching" ||
        stage === "aligning" ||
        stage === "report" ||
        stage === "depth" ||
        stage === "pointcloud" ||
        stage === "export"
      ) {
        return stage;
      }

      return null;
    }

    function updateProcessStages(job) {
      if (job.status === "queued" || job.status === "running" || job.status === "done" || job.status === "failed") {
        processStageWrap.style.display = "grid";
      } else {
        processStageWrap.style.display = "none";
      }

      resetStageClasses();

      const order = ["matching", "aligning", "report", "depth", "pointcloud", "export"];
      const currentStage = mapJobToStage(job);

      if (!currentStage && job.status !== "done" && job.status !== "failed") return;

      if (job.status === "done") {
        order.forEach((stage) => {
          const el = processStageWrap.querySelector(`[data-stage="${stage}"]`);
          if (el) el.classList.add("done");
        });
        return;
      }

      if (job.status === "failed") {
        const idx = currentStage ? order.indexOf(currentStage) : -1;
        order.forEach((stage, i) => {
          const el = processStageWrap.querySelector(`[data-stage="${stage}"]`);
          if (!el) return;
          if (idx >= 0 && i < idx) el.classList.add("done");
          if (stage === currentStage) el.classList.add("error");
        });
        return;
      }

      const currentIdx = currentStage ? order.indexOf(currentStage) : -1;

      order.forEach((stage, i) => {
        const el = processStageWrap.querySelector(`[data-stage="${stage}"]`);
        if (!el) return;
        if (currentIdx >= 0 && i < currentIdx) el.classList.add("done");
        if (stage === currentStage) el.classList.add("active");
      });
    }

    function clearSelectedFilesOnly() {
      files = [];
      totalSize = 0;
      latestDataset = null;
      latestExifSummary = {};
      latestMissionType = "Sin detectar";
      pendingProjectName = "";
      selectedHistoryJobId = "";
      selectedHistoryJobId = "";
      fileInput.value = "";

      dropTitle.textContent = "Arrastra tus fotos aquí";
      dropSubtitle.textContent =
        "para analizarlas, calcular tiempo y precio y configurar el procesado.";

      if (uploadValid) uploadValid.style.display = "none";
      if (uploadMeta) uploadMeta.style.display = "none";
      if (uploadPhotosMeta) uploadPhotosMeta.textContent = "Fotos analizadas: 0";
      if (uploadGpsMeta) uploadGpsMeta.textContent = "Extensión GPS: 0 m";

      datasetGrid.style.display = "none";
      hideDatasetHighlight();

      fieldSize.textContent = "0 MB";
      fieldResolution.textContent = "Sin detectar";
      fieldGps.textContent = "Sin detectar";
      fieldMission.textContent = "Sin detectar";
      fieldOverlap.textContent = "Sin detectar";
      fieldCamera.textContent = "Sin detectar";
      fieldDrone.textContent = "Sin detectar";
      fieldLocation.textContent = "Sin detectar";
      fieldLens.textContent = "Sin detectar";
      fieldFocal.textContent = "Sin detectar";
      fieldDate.textContent = "Sin detectar";

      if (uploadActions) uploadActions.style.display = "none";
      if (pricingPanel) pricingPanel.style.display = "none";
      if (mapWrap) mapWrap.style.display = "none";
      if (processStartWrap) processStartWrap.style.display = "none";

      if (map && marker) {
        map.removeLayer(marker);
        marker = null;
      }

      resetPreviewState();
      setAnalysisPlaceholder();
      jobStatusEl.textContent = "Puede subir otra selección manteniendo la calidad y los archivos de salida elegidos.";
      enableUploadButton();
    }

    function resetAll() {
      stopPolling();
      resetPreviewState();

      files = [];
      totalSize = 0;
      latestDataset = null;
      latestMissionType = "Sin detectar";
      pendingProjectName = "";
      fileInput.value = "";

      dropTitle.textContent = "Arrastra tus fotos aquí";
      dropSubtitle.textContent =
        "para analizarlas, calcular tiempo y precio y configurar el procesado.";

      if (mainLayout) mainLayout.classList.remove("with-data");
      if (uploadPanel) uploadPanel.style.display = "block";
      if (datasetPanel) datasetPanel.style.display = "none";
      if (topConfigRow) topConfigRow.style.display = "none";
      datasetGrid.style.display = "none";
      if (uploadActions) uploadActions.style.display = "none";
      if (processStartWrap) processStartWrap.style.display = "none";
      if (uploadBtn) {
        uploadBtn.textContent = "Iniciar procesado";
        uploadBtn.disabled = false;
      }
      if (pricingPanel) pricingPanel.style.display = "none";
      if (processPanel) processPanel.style.display = "none";
      if (mapWrap) mapWrap.style.display = "none";
      if (qualityModeWrap) qualityModeWrap.style.display = "none";
      if (outputsWrap) outputsWrap.style.display = "none";
      if (outputModeWrap) outputModeWrap.style.display = "none";
      if (selectedQualityRow) selectedQualityRow.style.display = "none";
      setSelectedQualityMode("normal", false);
      hideDatasetHighlight();

      fieldSize.textContent = "0 MB";
      fieldResolution.textContent = "Sin detectar";
      fieldGps.textContent = "Sin detectar";
      fieldMission.textContent = "Sin detectar";
      fieldOverlap.textContent = "Sin detectar";

      resetDetectionInfo();
      const mapEl = document.getElementById("map");
if (mapEl) {
  mapEl.style.height = "260px";
}

if (map) {
  setTimeout(() => {
    map.invalidateSize(true);
    if (baseLayer) baseLayer.redraw();
  }, 150);
}
      enableUploadButton();
      setAnalysisPlaceholder();
      hideDownloadButton();

      processStageWrap.style.display = "none";
      resetStageClasses();

      if (processSelectionSummary) {
        processSelectionSummary.style.display = "none";
        processSelectionSummary.innerHTML = "";
      }

      jobStatusEl.textContent = "Aquí se mostrará el estado real del trabajo una vez iniciado.";
      if (projectTitle) { projectTitle.style.display = "none"; projectTitle.textContent = ""; }
      if (uploadBtn) uploadBtn.textContent = "Iniciar procesado";

      progressWrap.style.display = "none";
      progressFill.style.width = "0%";
      progressFill.classList.remove("error");
      progressFill.style.background = "linear-gradient(90deg, var(--accent-2), var(--accent))";
      progressLabel.textContent = "Subiendo...";
      progressPercent.textContent = "0%";
    }

    function applyDatasetVisualState(dataset) {
      if (dataset.missingExifCore || dataset.blockers.length > 0) {
        hideDatasetHighlight();
        if (uploadValid) {
          uploadValid.style.display = "flex";
          uploadValid.style.color = "var(--danger)";
          uploadValid.textContent = "Dataset no válido para procesado";
        }
        disableUploadButton();
        return;
      }

      if (dataset.warnings.length > 0) {
        hideDatasetHighlight();
        if (uploadValid) {
          uploadValid.style.display = "flex";
          uploadValid.style.color = "var(--warning)";
          uploadValid.textContent = "Dataset con advertencias";
        }
        enableUploadButton();
        return;
      }

      hideDatasetHighlight();
      if (uploadValid) {
        uploadValid.style.display = "flex";
        uploadValid.style.color = "var(--ok)";
        uploadValid.textContent = "Dataset válido para fotogrametría";
      }
      enableUploadButton();
    }

    async function updateUI() {
      totalSize = files.reduce((acc, file) => acc + file.size, 0);

      if (files.length > 0) {
        if (mainLayout) mainLayout.classList.add("with-data");
        dropTitle.textContent = `${files.length} archivos seleccionados`;
        if (datasetPanel) datasetPanel.style.display = "block";
        if (topConfigRow) topConfigRow.style.display = "grid";
        datasetGrid.style.display = "grid";
        if (uploadActions) uploadActions.style.display = "flex";
        if (pricingPanel) pricingPanel.style.display = "block";
        if (processPanel) processPanel.style.display = "block";
        if (processStartWrap) processStartWrap.style.display = "flex";
        if (mapWrap) mapWrap.style.display = "block";
        if (qualityModeWrap) qualityModeWrap.style.display = "flex";
        if (outputsWrap) outputsWrap.style.display = "flex";
        if (outputModeWrap) outputModeWrap.style.display = "flex";
        if (selectedQualityRow) selectedQualityRow.style.display = "flex";

        refreshPricingPreviewDebounced();
        detectCamera(files[0]);

        const dataset = await analyzeDataset(files);
        latestExifSummary = {
          camera: dataset.cameras,
          megapixels: dataset.megapixels,
          gpsDetected: dataset.gpsDetected,
          gpsSpread: dataset.gpsSpread,
          overlap: dataset.overlap,
          warnings: dataset.warnings,
          blockers: dataset.blockers,
          missionType: detectMissionType(dataset),
          totalPhotos: files.length,
          totalSize: totalSize
        };
        const missionType = detectMissionType(dataset);

        latestDataset = dataset;
        latestMissionType = missionType;

        fillDatasetGrid(dataset, missionType);
        applyDatasetVisualState(dataset);

        dropSubtitle.textContent = "Las imágenes se han analizado correctamente.";
        if (uploadMeta) uploadMeta.style.display = "flex";
        if (uploadPhotosMeta) uploadPhotosMeta.textContent = `Fotos analizadas: ${Math.min(files.length, DATASET_SAMPLE_SIZE)}`;
        if (uploadGpsMeta) uploadGpsMeta.textContent = `Extensión GPS: ${dataset.gpsSpread?.maxDistanceMeters || 0} m`;

        setAnalysisPlaceholder();
      } else {
        resetAll();
    renderTopbarUser();
      }
    }

    function detectDrone(cameraText) {
      const c = (cameraText || "").toUpperCase();
      let drone = "Desconocido";

      if (c.includes("L1D-20C") || c.includes("L1D-20")) {
        drone = "DJI Mavic 2 Pro";
      } else if (c.includes("FC220")) {
        drone = "DJI Mavic Pro / Spark / Mavic Air";
      } else if (c.includes("FC6310")) {
        drone = "DJI Phantom 4 Pro";
      } else if (c.includes("FC6520")) {
        drone = "DJI Phantom 4 RTK";
      } else if (c.includes("M3E") || c.includes("FC3411")) {
        drone = "DJI Mavic 3 Enterprise";
      } else if (c.includes("M3T")) {
        drone = "DJI Mavic 3 Thermal";
      } else if (c.includes("M3P")) {
        drone = "DJI Mavic 3 Pro";
      } else if (c.includes("FC3582") || c.includes("FC3582C")) {
        drone = "DJI Air 2S";
      } else if (c.includes("FC8482")) {
        drone = "DJI Mini 3 / Mini 4 Pro";
      } else if (c.includes("P1")) {
        drone = "DJI Matrice + Zenmuse P1";
      } else if (c.includes("H20")) {
        drone = "DJI Matrice + Zenmuse H20";
      } else if (c.includes("M300")) {
        drone = "DJI Matrice 300 RTK";
      } else if (c.includes("M350")) {
        drone = "DJI Matrice 350 RTK";
      } else if (c.includes("AUTEL") && c.includes("EVO II")) {
        drone = "Autel EVO II";
      } else if (c.includes("EVO II RTK")) {
        drone = "Autel EVO II RTK";
      } else if (c.includes("ANAFI AI")) {
        drone = "Parrot Anafi Ai";
      } else if (c.includes("ANAFI")) {
        drone = "Parrot Anafi";
      } else if (c.includes("SKYDIO")) {
        drone = "Skydio (serie 2 / X2)";
      } else if (c.includes("SONY")) {
        drone = "Drone profesional con cámara Sony";
      } else if (c.includes("ZENMUSE")) {
        drone = "DJI Matrice con cámara Zenmuse";
      }

      fieldDrone.textContent = drone;
    }

    async function analyzeDataset(fileList) {
      const sample = fileList.slice(0, DATASET_SAMPLE_SIZE);

      const cameras = new Set();
      const widths = [];
      const heights = [];
      const gpsPoints = [];
      let gpsDetected = false;

      for (const file of sample) {
        const data = await readExif(file);
        if (!data) continue;

        const make = (data.Make || "").trim();
        const model = (data.Model || "").trim();
        const cameraName = `${make} ${model}`.trim();

        if (cameraName) {
          cameras.add(cameraName);
        }

        const width = Number(data.ExifImageWidth || data.PixelXDimension || 0);
        const height = Number(data.ExifImageHeight || data.PixelYDimension || 0);

        if (width > 0 && height > 0) {
          widths.push(width);
          heights.push(height);
        }

        if (data.GPSLatitude && data.GPSLongitude) {
          gpsDetected = true;

          const lat = convertGPS(data.GPSLatitude, data.GPSLatitudeRef);
          const lon = convertGPS(data.GPSLongitude, data.GPSLongitudeRef);

          if (
            lat !== null &&
            lon !== null &&
            !Number.isNaN(lat) &&
            !Number.isNaN(lon)
          ) {
            gpsPoints.push({ lat, lon });
          }
        }
      }

      const cameraCount = cameras.size;

      const avgWidth =
        widths.length > 0
          ? widths.reduce((a, b) => a + b, 0) / widths.length
          : 0;

      const avgHeight =
        heights.length > 0
          ? heights.reduce((a, b) => a + b, 0) / heights.length
          : 0;

      const megapixels =
        avgWidth > 0 && avgHeight > 0
          ? Number(((avgWidth * avgHeight) / 1000000).toFixed(1))
          : 0;

      const overlapInfo = estimateOverlapFromGps(gpsPoints);
      const gpsSpread = estimateGpsSpread(gpsPoints);

      const warnings = [];
      const blockers = [];

      const missingExifCore =
        cameras.size === 0 && megapixels === 0 && !gpsDetected;

      if (cameraCount > 1) {
        warnings.push("Se detectaron múltiples cámaras");
      }

      if (megapixels > 0 && megapixels < 8) {
        warnings.push("Resolución baja para fotogrametría");
      }

      if (!gpsDetected) {
        warnings.push("No se ha detectado GPS en la muestra analizada");
      }

      if (overlapInfo.status === "low") {
        warnings.push("El solape estimado parece bajo");
      }

      if (missingExifCore) {
        blockers.push("No se han encontrado metadatos EXIF suficientes");
      }

      if (megapixels > 0 && megapixels < 3) {
        blockers.push("La resolución es demasiado baja para un procesado fiable");
      }

      return {
        cameraCount,
        cameras: [...cameras],
        megapixels,
        gpsDetected,
        gpsPointsCount: gpsPoints.length,
        overlap: overlapInfo,
        gpsSpread,
        warnings,
        blockers,
        missingExifCore,
        isValidForProcessing: blockers.length === 0
      };
    }

    function estimateOverlapFromGps(points) {
      if (!points || points.length < 3) {
        return {
          status: "unknown",
          message: "No se ha podido estimar el solape"
        };
      }

      const distances = [];

      for (let i = 1; i < points.length; i++) {
        const d = haversineMeters(points[i - 1], points[i]);
        if (Number.isFinite(d) && d > 0) {
          distances.push(d);
        }
      }

      if (!distances.length) {
        return {
          status: "unknown",
          message: "No se ha podido estimar el solape"
        };
      }

      const avgDistance =
        distances.reduce((a, b) => a + b, 0) / distances.length;

      if (avgDistance <= 5) {
        return {
          status: "high",
          message: "Solape estimado alto"
        };
      }

      if (avgDistance <= 15) {
        return {
          status: "medium",
          message: "Solape estimado medio"
        };
      }

      return {
        status: "low",
        message: "Solape estimado bajo"
      };
    }

    function estimateGpsSpread(points) {
      if (!points || points.length < 2) {
        return {
          maxDistanceMeters: 0,
          spreadLabel: "desconocido"
        };
      }

      let maxDistance = 0;

      for (let i = 0; i < points.length; i++) {
        for (let j = i + 1; j < points.length; j++) {
          const d = haversineMeters(points[i], points[j]);
          if (Number.isFinite(d) && d > maxDistance) {
            maxDistance = d;
          }
        }
      }

      let spreadLabel = "bajo";
      if (maxDistance > 80) spreadLabel = "alto";
      else if (maxDistance > 25) spreadLabel = "medio";

      return {
        maxDistanceMeters: Math.round(maxDistance),
        spreadLabel
      };
    }

    function haversineMeters(a, b) {
      const R = 6371000;
      const toRad = (deg) => (deg * Math.PI) / 180;

      const dLat = toRad(b.lat - a.lat);
      const dLon = toRad(b.lon - a.lon);

      const lat1 = toRad(a.lat);
      const lat2 = toRad(b.lat);

      const x =
        Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.cos(lat1) *
          Math.cos(lat2) *
          Math.sin(dLon / 2) *
          Math.sin(dLon / 2);

      const c = 2 * Math.atan2(Math.sqrt(x), Math.sqrt(1 - x));

      return R * c;
    }

    function detectMissionType(dataset) {
      if (!dataset.gpsDetected) {
        return "Objeto / escaneo cercano";
      }

      const spread = Number(dataset.gpsSpread?.maxDistanceMeters || 0);
      const overlapStatus = dataset.overlap?.status || "unknown";
      const cameraCount = Number(dataset.cameraCount || 0);
      const mp = Number(dataset.megapixels || 0);

      if (spread > 0 && spread <= 30 && cameraCount === 1) {
        return "Inspección de torre / estructura vertical";
      }

      if (spread > 30 && spread <= 80 && overlapStatus !== "low") {
        return "Fachada / estructura";
      }

      if (spread > 80 && mp >= 8) {
        return "Fotogrametría aérea";
      }

      return "Escaneo general";
    }

    function readExif(file) {
      return new Promise((resolve) => {
        const reader = new FileReader();

        reader.onload = function (e) {
          const img = new Image();

          img.onload = function () {
            try {
              EXIF.getData(img, function () {
                resolve({
                  Make: EXIF.getTag(this, "Make"),
                  Model: EXIF.getTag(this, "Model"),
                  
                  DeviceModel: EXIF.getTag(this, "Model"),
                  DeviceMake: EXIF.getTag(this, "Make"),
                  LensModel: EXIF.getTag(this, "LensModel"),
                  FocalLength: EXIF.getTag(this, "FocalLength"),
                  DateTimeOriginal: EXIF.getTag(this, "DateTimeOriginal"),
                  
                  ExifImageWidth: EXIF.getTag(this, "ExifImageWidth"),
                  ExifImageHeight: EXIF.getTag(this, "ExifImageHeight"),
                  PixelXDimension: EXIF.getTag(this, "PixelXDimension"),
                  PixelYDimension: EXIF.getTag(this, "PixelYDimension"),
                  GPSLatitude: EXIF.getTag(this, "GPSLatitude"),
                  GPSLongitude: EXIF.getTag(this, "GPSLongitude"),
                  GPSLatitudeRef: EXIF.getTag(this, "GPSLatitudeRef"),
                  GPSLongitudeRef: EXIF.getTag(this, "GPSLongitudeRef")
                });
              });
            } catch (_) {
              resolve(null);
            }
          };

          img.onerror = function () {
            resolve(null);
          };

          reader.onerror = function () {
            resolve(null);
          };

          img.src = e.target.result;
        };

        reader.readAsDataURL(file);
      });
    }

    function detectCamera(file) {
      if (!file) {
        resetDetectionInfo();
        return;
      }

      const reader = new FileReader();

      reader.onload = function (e) {
        const img = new Image();

        img.onload = function () {
          try {
            EXIF.getData(img, function () {
              const make = EXIF.getTag(this, "Make") || "";
              const model = EXIF.getTag(this, "Model") || "";
              const lens = EXIF.getTag(this, "LensModel") || "No detectado";
              const focal = EXIF.getTag(this, "FocalLength");
              const date = EXIF.getTag(this, "DateTimeOriginal") || "No detectada";
              const camera = (make + " " + model).trim();

              if (camera) {
                fieldCamera.textContent = camera;
                detectDrone(camera);
                fieldLens.textContent = lens;

                if (focal && focal.numerator && focal.denominator) {
                  fieldFocal.textContent = (focal.numerator / focal.denominator).toFixed(1) + " mm";
                } else {
                  fieldFocal.textContent = "No detectada";
                }

                fieldDate.textContent = date;
              
              } else {
                fieldCamera.textContent = "No detectada";
                fieldDrone.textContent = "No detectado";
              }

              detectGPS(this);
            });
          } catch (_) {
            fieldCamera.textContent = "No detectada";
            fieldDrone.textContent = "No detectado";
            fieldLocation.textContent = "No detectada";
            if (files.length > 0) mapWrap.style.display = "block";
          }
        };

        img.onerror = function () {
          fieldCamera.textContent = "No detectada";
          fieldDrone.textContent = "No detectado";
          fieldLocation.textContent = "No detectada";
          if (files.length > 0) mapWrap.style.display = "block";
        };

        reader.onerror = function () {
          fieldCamera.textContent = "No detectada";
          fieldDrone.textContent = "No detectado";
          fieldLocation.textContent = "No detectada";
          if (files.length > 0) mapWrap.style.display = "block";
        };

        img.src = e.target.result;
      };

      reader.readAsDataURL(file);
    }

    function gpsPartToNumber(part) {
      if (typeof part === "number") return part;

      if (part && typeof part === "object") {
        if (
          typeof part.numerator === "number" &&
          typeof part.denominator === "number" &&
          part.denominator !== 0
        ) {
          return part.numerator / part.denominator;
        }
      }

      return 0;
    }

    function convertGPS(coord, ref) {
      if (!coord || coord.length < 3) return null;

      const d = gpsPartToNumber(coord[0]);
      const m = gpsPartToNumber(coord[1]);
      const s = gpsPartToNumber(coord[2]);

      let dec = d + (m / 60) + (s / 3600);

      if (ref === "S" || ref === "W") {
        dec *= -1;
      }

      return dec;
    }

    function showMap(lat, lon, zoom = 16, addMarker = true) {
  mapWrap.style.display = "block";

  const mapEl = document.getElementById("map");
  if (mapEl) {
    mapEl.style.height = addMarker ? "340px" : "260px";
  }

  if (!map) {
    map = L.map("map", {
      zoomControl: true,
      preferCanvas: true
    });

    baseLayer = L.tileLayer(
      "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
      {
        maxZoom: 19,
        attribution: "Esri, Maxar, Earthstar Geographics"
      }
    );

    baseLayer.on("tileerror", function () {
      jobStatusEl.textContent = "El mapa base no ha cargado correctamente";
    });

    baseLayer.addTo(map);
  }

  if (marker) {
    map.removeLayer(marker);
    marker = null;
  }

  if (addMarker) {
    marker = L.marker([lat, lon]).addTo(map);
  }

  map.setView([lat, lon], zoom);

  setTimeout(() => {
    map.invalidateSize(true);
    if (baseLayer) baseLayer.redraw();
  }, 200);

  setTimeout(() => {
    map.invalidateSize(true);
    if (baseLayer) baseLayer.redraw();
  }, 700);

  setTimeout(() => {
    map.invalidateSize(true);
    if (baseLayer) baseLayer.redraw();
  }, 1400);
}

     function initMapWorldView() {
  if (map) return;

  // España, zoom lejano, sin marcador
  showMap(40.4168, -3.7038, 4, false);
}
    
    async function reverseGeocode(lat, lon) {
      try {
        const url = `https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat=${encodeURIComponent(lat)}&lon=${encodeURIComponent(lon)}&zoom=12&addressdetails=1`;

        const response = await fetch(url, {
          headers: {
            "Accept": "application/json"
          }
        });

        if (!response.ok) {
          fieldLocation.textContent = "Ubicación no disponible";
          return;
        }

        const data = await response.json();
        const address = data.address || {};

        const municipality =
          address.city ||
          address.town ||
          address.village ||
          address.municipality ||
          address.hamlet ||
          address.county ||
          "";

        const province =
          address.state ||
          address.province ||
          address.region ||
          "";

        const country = address.country || "";

        const parts = [municipality, province, country].filter(Boolean);

        if (parts.length > 0) {
          fieldLocation.textContent = parts.join(", ");
        } else if (data.display_name) {
          fieldLocation.textContent = data.display_name;
        } else {
          fieldLocation.textContent = "Ubicación no disponible";
        }
      } catch (_) {
        fieldLocation.textContent = "Ubicación no disponible";
      }
    }

    async function detectGPS(exifContext) {
      const lat = EXIF.getTag(exifContext, "GPSLatitude");
      const lon = EXIF.getTag(exifContext, "GPSLongitude");
      const latRef = EXIF.getTag(exifContext, "GPSLatitudeRef");
      const lonRef = EXIF.getTag(exifContext, "GPSLongitudeRef");

      if (!lat || !lon || !latRef || !lonRef) {
        fieldLocation.textContent = "No detectada";
        return;
      }

      const latitude = convertGPS(lat, latRef);
      const longitude = convertGPS(lon, lonRef);

      if (
        latitude === null ||
        longitude === null ||
        Number.isNaN(latitude) ||
        Number.isNaN(longitude)
      ) {
        fieldLocation.textContent = "No detectada";
        return;
      }

      fieldLocation.textContent = "Buscando ubicación...";
      showMap(latitude, longitude);
      await reverseGeocode(latitude, longitude);
    }

    async function setFiles(fileList) {
      files = Array.from(fileList || []).filter((file) => {
        const name = file.name.toLowerCase();
        return (
          name.endsWith(".jpg") ||
          name.endsWith(".jpeg") ||
          name.endsWith(".png") ||
          name.endsWith(".tif") ||
          name.endsWith(".tiff") ||
          name.endsWith(".dng")
        );
      });

      statusEl.textContent = "";
      hideDownloadButton();
      await updateUI();
    }

    drop.addEventListener("click", (e) => {
      const interactive = e.target.closest("button, input, select, a, .quality-mode-wrap, .actions");
      if (interactive) return;
      fileInput.click();
    });

    drop.addEventListener("dragover", (e) => {
      e.preventDefault();
      drop.classList.add("dragover");
    });

    drop.addEventListener("dragleave", () => {
      drop.classList.remove("dragover");
    });

    drop.addEventListener("drop", async (e) => {
      e.preventDefault();
      drop.classList.remove("dragover");
      await setFiles(e.dataTransfer.files);
    });

    fileInput.addEventListener("change", async (e) => {
      await setFiles(e.target.files);
    });

    clearBtn.addEventListener("click", async () => {
      if (files.length === 0) {
        return;
      }

      const ok = await openModal({
        title: "Eliminar fotos",
        subtitle: "Se eliminarán solo las imágenes cargadas.",
        body: `
          <div class="modal-note">
            Se borrarán únicamente las fotografías seleccionadas. La calidad y los archivos de salida elegidos se mantendrán.
          </div>
        `,
        confirmText: "Sí, eliminar fotos",
        cancelText: "Cancelar",
        showCancel: true,
        tone: "warning"
      });

      if (ok) {
        clearSelectedFilesOnly();
      }
    });

    uploadBtn.addEventListener("click", async () => {
      if (uploadBtn.textContent === "Iniciar nuevo proyecto") {
        resetAll();
        window.scrollTo({ top: 0, behavior: "smooth" });
        return;
      }
      const currentUser = getStoredUser();

      if (!currentUser || !currentUser.email) {
        await showInfoModal(
          "Debe iniciar sesión",
          "Acceso requerido",
          "Debe iniciar sesión antes de proseguir.",
          "warning"
        );
        return;
      }

      await upload();
    });

    async function createJob() {
      const cleanExifSummary = stripNullCharsDeep({
        ...(latestExifSummary || {}),
        _xproces: buildXprocesMeta()
      });

      const response = await fetch(`${API_BASE}/jobs`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
  exif_summary: cleanExifSummary,
  client_email: getStoredUser()?.email || "",
  project_name: pendingProjectName,
  client_name: getStoredUser()?.name || "",
  user_id: getStoredUser()?.id || null,
  quality_mode: getSelectedQualityMode(),
  outputs_requested: getSelectedOutputs(),
  tams_export: isTamsPresetSelected()
})
      });

      if (!response.ok) {
        const text = await response.text().catch(() => "");
        throw new Error(`No se pudo crear el job (${response.status}) ${text}`);
      }

      return response.json();
    }

    async function uploadBatch(jobId, batchFiles, batchIndex, totalBatches) {
      return new Promise((resolve, reject) => {
        const form = new FormData();
        batchFiles.forEach((file) => form.append("photos", file));

        const xhr = new XMLHttpRequest();
        xhr.open("POST", `${API_BASE}/jobs/${jobId}/upload`, true);

        xhr.upload.onprogress = function (e) {
          if (e.lengthComputable) {
            const batchPercent = e.loaded / e.total;
            const overallPercent = ((batchIndex + batchPercent) / totalBatches) * 100;
            const rounded = Math.round(overallPercent);

            progressFill.style.width = `${rounded}%`;
            progressLabel.textContent = `Subiendo lote ${batchIndex + 1} de ${totalBatches}...`;
            progressPercent.textContent = `${rounded}%`;
          }
        };

        xhr.onload = function () {
          if (xhr.status >= 200 && xhr.status < 300) {
            resolve();
          } else {
            reject(new Error(`Error subiendo lote ${batchIndex + 1}: ${xhr.status}`));
          }
        };

        xhr.onerror = function () {
          reject(new Error(`Error de red en lote ${batchIndex + 1}`));
        };

        xhr.send(form);
      });
    }

    async function submitJob(jobId) {
      const response = await fetch(`${API_BASE}/jobs/${jobId}/submit`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        }
      });

      if (!response.ok) {
        const text = await response.text().catch(() => "");
        throw new Error(`No se pudo enviar el job a procesado (${response.status}) ${text}`);
      }

      return response.json();
    }

    async function getJob(jobId) {
      const response = await fetch(`${API_BASE}/jobs/${jobId}`, {
        cache: "no-store"
      });

      if (!response.ok) {
        throw new Error(`No se pudo consultar el job (${response.status})`);
      }

      return response.json();
    }

    async function getJobEta(jobId) {
      try {
        const response = await fetch(`${API_BASE}/jobs/${jobId}/eta`, {
          cache: "no-store"
        });
        if (!response.ok) return null;
        return await response.json();
      } catch (_) {
        return null;
      }
    }



    async function getJobLog(jobId) {
      try {
        const response = await fetch(`${API_BASE}/jobs/${jobId}/log`, {
          cache: "no-store"
        });
        if (!response.ok) return null;
        return await response.text();
      } catch (_) {
        return null;
      }
    }

    function escapeHtml(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function formatDateTime(value) {
      if (!value) return "No disponible";
      const d = new Date(value);
      if (Number.isNaN(d.getTime())) return "No disponible";
      return d.toLocaleString("es-ES");
    }

    function formatDurationSeconds(value) {
      const total = Math.max(0, Number(value || 0));
      if (!total) return "No disponible";
      const hours = Math.floor(total / 3600);
      const minutes = Math.floor((total % 3600) / 60);
      const seconds = Math.floor(total % 60);

      if (hours > 0) return `${hours} h ${minutes} min ${seconds} s`;
      if (minutes > 0) return `${minutes} min ${seconds} s`;
      return `${seconds} s`;
    }

    function parseProcessLog(text) {
      const info = {
        quality: "",
        photos: "",
        requestedOutputs: [],
        generatedOutputs: [],
        omittedOutputs: [],
        params: "",
        steps: [],
        alignedCameras: "",
        warnings: []
      };

      if (!text) return info;

      const lines = String(text).split(/\r?\n/).map((line) => line.trim()).filter(Boolean);

      for (const line of lines) {
        if (line.startsWith("Modo de calidad:")) {
          info.quality = line.replace("Modo de calidad:", "").trim().replace(/Metashape/gi, "Xproces");
          continue;
        }

        if (line.startsWith("Fotos encontradas:")) {
          info.photos = line.replace("Fotos encontradas:", "").trim();
          continue;
        }

        if (line.startsWith("Salidas solicitadas:")) {
          info.requestedOutputs = line
            .replace("Salidas solicitadas:", "")
            .split(",")
            .map((x) => x.trim())
            .filter(Boolean);
          continue;
        }

        if (line.startsWith("Aviso:")) {
          info.warnings.push(line.replace("Aviso:", "").trim().replace(/Metashape/gi, "Xproces"));
          const omittedMatch = line.match(/se omitirán:\s*(.*)$/i);
          if (omittedMatch) {
            info.omittedOutputs = omittedMatch[1].split(",").map((x) => x.trim()).filter(Boolean);
          }
          continue;
        }

        if (line.startsWith("Parámetros:")) {
          info.params = line.replace("Parámetros:", "").trim();
          continue;
        }

        if (line.startsWith("Cámaras alineadas:")) {
          info.alignedCameras = line.replace("Cámaras alineadas:", "").trim();
          continue;
        }

        if (line.includes(" exportado")) {
          const name = line.replace(" exportado", "").trim();
          if (name) info.generatedOutputs.push(name);
          continue;
        }

        if (line.startsWith("XPROCES_PROGRESS|")) {
          const parts = line.split("|");
          if (parts.length >= 3) {
            info.steps.push(parts[2].trim().replace(/Metashape/gi, "Xproces"));
          }
          continue;
        }
      }

      info.generatedOutputs = [...new Set(info.generatedOutputs)];
      info.requestedOutputs = [...new Set(info.requestedOutputs)];
      info.omittedOutputs = [...new Set(info.omittedOutputs)];
      info.steps = [...new Set(info.steps)];

      return info;
    }

    function renderProcessSummary(job, logText) {
      const parsed = parseProcessLog(logText);

      const normalizeOutputKey = (value) => {
        const raw = String(value || "").trim().toLowerCase();
        const aliases = {
          "obj": "mesh_obj",
          "pdf": "pdf_report",
          "pdf report": "pdf_report",
          "report": "pdf_report",
          "report.pdf": "pdf_report",
          "las": "las",
          "laz": "laz",
          "e57": "e57",
          "ply": "ply",
          "pts": "pts",
          "dem": "dem_tif",
          "dem.tif": "dem_tif",
          "orthomosaic": "orthomosaic_tif",
          "orthomosaic.tif": "orthomosaic_tif",
          "orthomosaic.jpg": "orthomosaic_jpg",
          "ortomosaico": "orthomosaic_tif",
          "ortomosaico jpg": "orthomosaic_jpg",
          "ortomosaico tif": "orthomosaic_tif",
          "mesh obj": "mesh_obj",
          "malla obj": "mesh_obj",
          "model.obj": "mesh_obj",
          "fbx": "mesh_fbx",
          "stl": "mesh_stl",
          "dae": "mesh_dae",
          "3ds": "mesh_3ds",
          "wrl": "mesh_wrl",
          "mesh ply": "mesh_ply",
          "malla ply": "mesh_ply"
        };
        return aliases[raw] || raw;
      };

      const qualityText =
        parsed.quality ||
        (job?.quality_mode ? formatQualityModeLabel(String(job.quality_mode).toLowerCase()) : "No disponible");

      const photosText = parsed.photos || "No disponible";

      const requestedRaw = parsed.requestedOutputs.length ? parsed.requestedOutputs : [];
      const requestedLabels = requestedRaw.map((item) => formatOutputLabel(String(item).trim().toLowerCase()));

      const generatedRaw = parsed.generatedOutputs.length ? parsed.generatedOutputs : [];
      const generatedLabels = generatedRaw.map((item) => formatOutputLabel(String(item).trim().toLowerCase()));

      const omittedLabels = parsed.omittedOutputs.length
        ? parsed.omittedOutputs.map((item) => formatOutputLabel(String(item).trim().toLowerCase()))
        : [];

      const requestedKeys = requestedRaw.map(normalizeOutputKey);
      const generatedKeys = generatedRaw.map(normalizeOutputKey);

      const missingFromResult = requestedRaw
        .filter((item) => !generatedKeys.includes(normalizeOutputKey(item)))
        .map((item) => formatOutputLabel(String(item).trim().toLowerCase()));

      const missingLabels = [...new Set([...omittedLabels, ...missingFromResult])];

      const warningLines = [];
      if (missingLabels.length) {
        warningLines.push(`Faltan: ${missingLabels.join(", ")}`);
      }

      parsed.warnings.forEach((w) => {
        const clean = w.replace(/estas salidas aún no se generan directamente con Xproces y se omitirán:\s*/i, "").trim();
        if (clean && !warningLines.some((x) => x.includes(clean))) {
          warningLines.push(clean);
        }
      });

      const statusValue = String(job?.status || "").toLowerCase();
      const failed = statusValue === "failed" || statusValue === "cancelled";
      if (failed && !warningLines.length) {
        warningLines.push("El procesado no se completó correctamente.");
      }

      const generatedText = generatedLabels.length ? generatedLabels.join(" · ") : "No disponible";
      const alignedText = parsed.alignedCameras ? `Cámaras alineadas: ${parsed.alignedCameras}` : "Cámaras alineadas: No disponible";
      const stateText = warningLines.length ? `⚠ ${warningLines.join(" · ")}` : "✔ Todo correcto";
      const titleText = failed ? "✖ Procesado fallido" : "✔ Procesado completado";

      return `
        <div class="process-summary">
          <div class="process-summary-hero">
            <div class="process-summary-title">${titleText}</div>
            <div class="process-summary-subtitle">${photosText} fotos · Calidad ${qualityText}</div>
          </div>

          <div class="process-summary-section">
            <div class="process-summary-section-title">Archivos generados</div>
            <div class="process-summary-list">${generatedText}</div>
          </div>

          <div class="process-summary-section">
            <div class="process-summary-section-title">Resumen</div>
            <div class="process-summary-list">${alignedText}</div>
          </div>

          <div class="process-summary-section">
            <div class="process-summary-section-title">Estado</div>
            <div class="process-summary-list">${stateText}</div>
          </div>
        </div>
      `;
    }

    function buildProjectStatusText(job) {
      const status = String(job?.status ?? "").toLowerCase();
      const stage = String(job?.stage ?? "");
      const message = String(job?.message ?? "").trim();
      const lines = [];

      if (status === "failed") lines.push("Estado: Fallido");
      else if (status === "cancelled") lines.push("Estado: Cancelado");
      else if (status === "completed") lines.push("Estado: Completado");
      else if (status === "queued") lines.push("Estado: En cola");
      else if (status === "running") lines.push("Estado: Procesando");
      else if (status) lines.push(`Estado: ${translateStatus(status)}`);

      if (stage) lines.push(`Fase: ${stage}`);
      if (message) lines.push(message);

      return lines.join("\n");
    }

    async function buildQueuedOrRunningMessage(job) {
      const eta = await getJobEta(job.id);

      if (!eta || !eta.ok) {
        return `Job ${job.id}\nEstado: ${job.status}\n${job.message || ""}`.trim();
      }

      if (job.status === "queued") {
        return `Job ${job.id}\nEstado: en cola\nSu proyecto está en cola.\nTiempo estimado: ${eta.total_estimated_human}`;
      }

      if (job.status === "running") {
        const remaining = Math.max(
          0,
          Number(eta.total_estimated_seconds || 0) - Number(job.total_seconds || 0)
        );

        const remainingText =
          remaining > 0 ? formatEtaText(remaining) : eta.total_estimated_human;

        return `Job ${job.id}\nEstado: procesando\n${job.message || "Procesando proyecto"}\nTiempo restante estimado: ${remainingText}`;
      }

      return `Job ${job.id}\nEstado: ${job.status}\n${job.message || ""}`.trim();
    }

    function startPolling(jobId) {
      stopPolling();

      pollTimer = setInterval(async () => {
        try {
          const job = await getJob(jobId);

          const progress = Number(job.progress ?? 0);
          const status = String(job.status ?? "").toLowerCase();
          if (datasetPanel) datasetPanel.style.display = "none";
          if (pricingPanel) pricingPanel.style.display = "none";
          if (mapWrap) mapWrap.style.display = "none";
          if (uploadPanel) uploadPanel.style.display = "none";
          if (processPanel) processPanel.style.display = "block";
          const message = job.message || "";

          progressWrap.style.display = "block";
          progressFill.classList.remove("error");
          progressFill.style.background = "";
          progressFill.style.width = `${Math.max(0, Math.min(100, progress))}%`;
          progressFill.classList.toggle("error", ["failed", "cancelled"].includes(status));
          if (status === "failed") progressLabel.textContent = "Fallido";
          else if (status === "cancelled") progressLabel.textContent = "Cancelado";
          else if (status === "completed" || status === "done") progressLabel.textContent = "Procesado completado";
          else if (status === "queued") progressLabel.textContent = "En cola";
          else progressLabel.textContent = "Procesando";
          progressPercent.textContent = `${Math.max(0, Math.min(100, progress))}%`;

          updateProcessStages(job);
          renderStoredJobs();

          if (status === "queued" || status === "running") {
            const eta = await getJobEta(job.id);

            if (eta && eta.ok) {
              etaBig.textContent = eta.total_estimated_human || "No disponible";
            }

            jobStatusEl.textContent = buildProjectStatusText(job);
          } else {
            jobStatusEl.textContent = buildProjectStatusText(job);
          }

          if (status === "done" || status === "completed" || status === "failed") {
            stopPolling();

            progressFill.style.width = "100%";
            progressLabel.textContent = "Trabajo procesado correctamente";
            progressPercent.textContent = "100%";

            const logText = await getJobLog(job.id);
            jobStatusEl.innerHTML = renderProcessSummary(job, logText);

            showDownloadButton(job.id);
          }

          if (status === "failed") {
            stopPolling();

            progressFill.style.width = "100%";
            progressFill.classList.add("error");
            progressFill.style.background = "linear-gradient(90deg, #ff7a7a, #ff5252)";
            progressLabel.textContent = "Procesado fallido";
            progressPercent.textContent = "100%";

            const logText = await getJobLog(job.id);

            if (logText) {
              jobStatusEl.innerHTML = renderProcessSummary(job, logText);
            } else {
              const rawError = `${job.error || ""} ${job.message || ""}`.toLowerCase();

              let msg = "El procesado no se pudo completar.";

              if (rawError.includes("no_input_files")) {
                msg = "No se encontraron imágenes válidas en el proyecto.";
              } else if (
                rawError.includes("muy pocas cámaras alineadas") ||
                rawError.includes("cámaras alineadas")
              ) {
                msg = "No hay suficiente solape entre las fotos para generar el modelo.";
              } else if (
                rawError.includes("point cloud densa") ||
                rawError.includes("depth maps")
              ) {
                msg = "No se ha podido generar la nube de puntos. Revise calidad, solape y resolución de las fotos.";
              }

              jobStatusEl.textContent =
                `Job ${job.id}\nEstado: error\n${msg}`;
              hideDownloadButton();
              return;
            }

            showDownloadButton(job.id);
          }
        } catch (error) {
          console.error("Polling error:", error);
        }
      }, POLL_INTERVAL_MS);
    }

    async function upload() {

        const currentUser = getStoredUser();

        if (!currentUser || !currentUser.email) {
          await showInfoModal(
            "Debe iniciar sesión",
            "Acceso requerido",
            "Debe iniciar sesión antes de proseguir.",
            "warning",
            { hideAuthWhileOpen: true }
          );
          return;
        }
      if (files.length === 0) {
        await showInfoModal(
          "No hay fotos seleccionadas",
          "Debe subir imágenes antes de iniciar el procesado.",
          "Seleccione o arrastre las fotografías originales del dron para poder calcular el precio, el tiempo y preparar el proyecto.",
          "warning"
        );
        return;
      }

      if (uploadBtn.disabled) {
        const motivos = [];
        if (latestDataset?.missingExifCore) {
          motivos.push("Faltan datos EXIF esenciales en las fotos.");
        }
        if (Array.isArray(latestDataset?.blockers) && latestDataset.blockers.length > 0) {
          motivos.push(...latestDataset.blockers);
        }

        await showInfoModal(
          "Dataset no válido para procesado",
          "No se puede iniciar el procesado con las fotos actuales.",
          motivos.length
            ? motivos.join("\n")
            : "Revise el dataset cargado y vuelva a intentarlo.",
          "error"
        );
        return;
      }

      if (previewTimer) {
        clearTimeout(previewTimer);
        previewTimer = null;
      }
      previewRequestId++;

      let preview = lastPreview;

      if (!preview || !preview.ok) {
        preview = await fetchPricingPreview(files.length, totalSize);
      }

      if (!preview || !preview.ok) {
        await showInfoModal(
          "No se pudo calcular la previsualización",
          "Precio y tiempo no están disponibles en este momento.",
          "Revise la conexión con la API e inténtelo de nuevo.",
          "error"
        );
        return;
      }

      lastPreview = preview;

      const timeText = preview.estimated_human || "No disponible";
      const priceText = `${preview.price} €`;

      const ok = await openModal({
        title: "Confirmar procesado",
        subtitle: "Revise el resumen antes de enviar el proyecto.",
        body: `
          <div class="modal-grid">
            <div class="modal-card">
              <div class="modal-label">Fotos</div>
              <div class="modal-value">${files.length}</div>
            </div>
            <div class="modal-card">
              <div class="modal-label">Tamaño total</div>
              <div class="modal-value">${formatBytes(totalSize)}</div>
            </div>
            <div class="modal-card">
              <div class="modal-label">Precio</div>
              <div class="modal-value">${priceText}</div>
            </div>
            <div class="modal-card">
              <div class="modal-label">Tiempo</div>
              <div class="modal-value">${timeText}</div>
            </div>
            <div class="modal-card">
              <div class="modal-label">Calidad</div>
              <div class="modal-value">${formatQualityModeLabel(getSelectedQualityMode())}</div>
            </div>
            <div class="modal-card full">
              <div class="modal-label">Archivos de salida</div>
              <div class="modal-value">${formatSelectedOutputsText()}</div>
            </div>
            <div class="modal-card full">
              <div class="modal-label">Nombre del proyecto</div>
              <input
                id="modalProjectName"
                type="text"
                placeholder="Ej: Estación 000564 "
                style="width:100%; min-height:44px; border:1px solid var(--border); border-radius:12px; padding:0 12px; font-size:14px; background:white;"
              >
            </div>
            <div class="modal-card full">
              <div class="modal-note">
                Se creará un nuevo proyecto, se subirán las fotografías al servidor y el trabajo entrará en cola para su procesado automático.
              </div>
            </div>
          </div>
        `,
        confirmText: "Sí, iniciar procesado",
        cancelText: "Cancelar",
        showCancel: true,
        tone: "default"
      });

      if (!ok) return;

      const projectNameValue = document.getElementById("modalProjectName")?.value?.trim() || "";

      if (!projectNameValue) {
        await showInfoModal(
          "Falta el nombre del proyecto",
          "Debe indicar un nombre para identificar el trabajo.",
          "Introduzca el nombre del proyecto antes de iniciar el procesado.",
          "warning"
        );
        return;
      }

      const selectedOutputs = typeof getSelectedOutputs === "function" ? getSelectedOutputs() : [];
      if (!selectedOutputs.length) {
        await showInfoModal(
          "Faltan archivos de salida",
          "Debe seleccionar qué archivos quiere recibir antes de iniciar el procesado.",
          "Elija un paquete recomendado o use la selección manual y marque al menos un archivo.",
          "warning"
        );
        return;
      }

      pendingProjectName = projectNameValue;

      try {
        stopPolling();
        hideDownloadButton();
        processStageWrap.style.display = "grid";
        resetStageClasses();

        progressWrap.style.display = "block";
        progressFill.style.width = "0%";
        progressFill.classList.remove("error");
      progressFill.style.background = "linear-gradient(90deg, var(--accent-2), var(--accent))";
        progressLabel.textContent = "Creando proyecto...";
        progressPercent.textContent = "0%";
        jobStatusEl.textContent = "Creando job...";

        const createdJob = await createJob();
        const jobId = createdJob.id;
        renderStoredJobs();

        if (projectTitle) { projectTitle.style.display = "block"; projectTitle.textContent = "Proyecto: " + (pendingProjectName || "Sin nombre"); }

        if (uploadPanel) uploadPanel.style.display = "none";
        if (datasetPanel) datasetPanel.style.display = "none";
        if (pricingPanel) pricingPanel.style.display = "none";
        if (mapWrap) mapWrap.style.display = "none";
        if (processPanel) processPanel.style.display = "block";
        updateProcessSelectionSummary();

        jobStatusEl.textContent = "Preparando subida por lotes...";

        const batches = chunkArray(files, BATCH_SIZE);

        for (let i = 0; i < batches.length; i++) {
          await uploadBatch(jobId, batches[i], i, batches.length);
        }

        progressFill.style.width = "100%";
        progressLabel.textContent = "Subida completada";
        progressPercent.textContent = "100%";
        jobStatusEl.textContent = "Subida completada. Enviando a procesado...";

        await submitJob(jobId);
        if (uploadBtn) uploadBtn.textContent = "Iniciar nuevo proyecto";
        if (datasetPanel) datasetPanel.style.display = "none";

        jobStatusEl.textContent = "Estado: En cola\nCalculando tiempo restante...";
        progressLabel.textContent = "En cola...";
        progressPercent.textContent = "0%";
        if (datasetPanel) datasetPanel.style.display = "none";
        startPolling(jobId);
      } catch (error) {
        console.error(error);
        jobStatusEl.textContent = error.message || "Error durante la subida";
        progressFill.classList.add("error");
        progressLabel.textContent = "Fallido";
      }
    }

      resetAll();
renderStoredJobs();
renderTopbarUser();
syncStoredUserToForm();

setTimeout(async () => {
  try {
    const savedJobId = getSavedSelectedHistoryJobId();
    const user = typeof getStoredUser === "function" ? getStoredUser() : null;
    if (!savedJobId || !user || !user.id) return;

    const response = await fetch(`${API_BASE}/jobs/mine`, {
      headers: {
        "x-user-id": String(user.id)
      },
      cache: "no-store"
    });

    const data = await response.json().catch(() => ({}));
    const jobs = Array.isArray(data.jobs) ? data.jobs : [];
    const savedJob = jobs.find((job) => String(job.id || "") === String(savedJobId));

    if (savedJob) {
      selectedHistoryJobId = savedJobId;
      await openJobFromHistory(savedJob);
      await renderStoredJobs();
    }
  } catch (_) {}
}, 0);

window.addEventListener("load", () => {
  initMapWorldView();
});
  </script>
</body>
</html>
om "ioredis";
import { Queue } from "bullmq";
import pkg from "pg";
import archiver from "archiver";
import cors from "cors";
import bcrypt from "bcryptjs";
import { randomBytes } from "crypto";


const { Pool } = pkg;

const app = express();
app.use(express.json());
app.use(cors());


// =====================
// CONFIG
// =====================
const DATA_ROOT = process.env.DATA_ROOT || "/data/mapxion";
const WORKER_TOKEN = process.env.WORKER_TOKEN || "";

let jobsHasQualityMode = false;

async function refreshSchemaFlags() {
  try {
    const result = await pool.query(
      `select exists (
         select 1
         from information_schema.columns
         where table_name = 'jobs'
           and column_name = 'quality_mode'
       ) as exists`
    );
    jobsHasQualityMode = !!result.rows?.[0]?.exists;
    console.log("Schema jobs.quality_mode:", jobsHasQualityMode ? "sí" : "no");
  } catch (e) {
    jobsHasQualityMode = false;
    console.error("No se pudo comprobar jobs.quality_mode", e?.message || e);
  }
}

function normalizeQualityMode(value) {
  const raw = String(value || "").trim().toLowerCase();

  if (!raw) return "normal";
  if (["rapido", "rápido", "fast"].includes(raw)) return "fast";
  if (["normal", "standard", "estandar", "estándar"].includes(raw)) return "normal";
  if (["max", "maximum", "maxima", "máxima", "ultra"].includes(raw)) return "max";

  return "normal";
}

function getQualityModeLabel(mode) {
  const normalized = normalizeQualityMode(mode);
  if (normalized === "fast") return "Rápido";
  if (normalized === "max") return "Máxima calidad";
  return "Normal";
}

function getQualityModeTimeFactor(mode) {
  const normalized = normalizeQualityMode(mode);
  if (normalized === "fast") return 0.65;
  if (normalized === "max") return 2.2;
  return 1;
}

function getQualityModePriceFactor(mode) {
  const normalized = normalizeQualityMode(mode);
  if (normalized === "fast") return 0.85;
  if (normalized === "max") return 1.75;
  return 1;
}

// =====================
// AUTH WORKER
// =====================
function isValidEmail(email) {
  const value = String(email || "").trim();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

function generateInviteCode() {
  return randomBytes(4).toString("hex").toUpperCase();
}


function stripNullCharsDeep(value) {
  if (typeof value === "string") {
    return value.replace(/\u0000/g, "").replace(/\x00/g, "").replace(/\0/g, "");
  }

  if (Array.isArray(value)) {
    return value.map(stripNullCharsDeep);
  }

  if (value && typeof value === "object") {
    const out = {};
    for (const [key, val] of Object.entries(value)) {
      out[key] = stripNullCharsDeep(val);
    }
    return out;
  }

  return value;
}


function requireWorkerAuth(req, res, next) {
  const auth = req.headers.authorization || "";

  if (!WORKER_TOKEN) {
    return res
      .status(500)
      .json({ ok: false, error: "WORKER_TOKEN not configured" });
  }

  if (auth !== `Bearer ${WORKER_TOKEN}`) {
    return res.status(401).json({ ok: false, error: "unauthorized" });
  }

  next();
}

// =====================
// POSTGRES
// =====================
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

pool
  .query("select 1")
  .then(async () => {
    console.log("Postgres conectado");
    await refreshSchemaFlags();
  })
  .catch((err) => console.error("Error Postgres", err));

// =====================
// REDIS + QUEUE
// =====================
let redis = null;
let redisReady = false;
let processQueue = null;

if (process.env.REDIS_URL) {
  redis = new IORedis(process.env.REDIS_URL, { maxRetriesPerRequest: null });

  redis.on("ready", () => {
    redisReady = true;
    console.log("Redis conectado (ready)");
  });

  redis.on("error", (e) => {
    redisReady = false;
    console.error("Redis error", e?.message || e);
  });

  processQueue = new Queue("processQueue", { connection: redis });
} else {
  console.log("REDIS_URL no definido");
}

// =====================
// DRIVE (/data)
// =====================
const jobDir = (jobId) => path.join(DATA_ROOT, "jobs", jobId);
const inputDir = (jobId) => path.join(jobDir(jobId), "input");
const outputDir = (jobId) => path.join(jobDir(jobId), "output");

function ensureJobDirs(jobId) {
  fs.mkdirSync(inputDir(jobId), { recursive: true });
  fs.mkdirSync(outputDir(jobId), { recursive: true });
}

// ✅ helper: lista SOLO imágenes (evita mezclar txt, etc.)
function listInputImages(jobId) {
  const dir = inputDir(jobId);
  if (!fs.existsSync(dir)) return [];
  const exts = new Set([".jpg", ".jpeg", ".png", ".tif", ".tiff", ".webp"]);
  return fs
    .readdirSync(dir)
    .filter((name) => exts.has(path.extname(name).toLowerCase()))
    .sort();
}

function getInputTotalBytes(jobId) {
  const dir = inputDir(jobId);
  if (!fs.existsSync(dir)) return 0;

  return fs.readdirSync(dir).reduce((acc, name) => {
    const full = path.join(dir, name);
    const stat = fs.statSync(full);
    if (stat.isFile()) return acc + stat.size;
    return acc;
  }, 0);
}

function estimateProcessingSecondsFromInputs(photosCount, totalBytes, qualityMode = "normal") {
  const photos = Number(photosCount || 0);
  const bytes = Number(totalBytes || 0);
  const gb = bytes / (1024 * 1024 * 1024);
  const factor = getQualityModeTimeFactor(qualityMode);

  return Math.round(
    (120 + (photos * 18) + (gb * 420)) * factor
  );
}

function calculatePriceFromInputs(photosCount, totalBytes, estimatedSeconds, qualityMode = "normal") {
  const photos = Number(photosCount || 0);
  const bytes = Number(totalBytes || 0);
  const gb = bytes / (1024 * 1024 * 1024);
  const hours = Number(estimatedSeconds || 0) / 3600;
  const factor = getQualityModePriceFactor(qualityMode);

  const base = 3;
  const byPhoto = photos * 0.03;
  const byGb = gb * 1.5;
  const byTime = hours * 6;

  return Math.ceil((base + byPhoto + byGb + byTime) * factor);
}

// ✅ helper: status “bloqueado” (no permitir más uploads)
function isLockedStatus(status) {
  return ["queued", "running", "done"].includes(
    String(status || "").toLowerCase()
  );
}

// =====================
// MULTER INPUT (photos)
// =====================
const inputStorage = multer.diskStorage({
  destination: (req, _file, cb) => {
    const { id } = req.params;
    ensureJobDirs(id);
    cb(null, inputDir(id));
  },
  filename: (_req, file, cb) => {
    const safe = file.originalname.replace(/[^\w.\-() ]+/g, "_");
    cb(null, safe);
  },
});

const uploadInput = multer({
  storage: inputStorage,
  limits: { fileSize: 250 * 1024 * 1024 }, // 250MB/foto
});

// =====================
// MULTER OUTPUT (zip subido por worker Windows)
// =====================
const outputStorage = multer.diskStorage({
  destination: (req, _file, cb) => {
    const { id } = req.params;
    ensureJobDirs(id);
    cb(null, outputDir(id));
  },
  filename: (_req, file, cb) => {
    const safe = file.originalname.replace(/[^\w.\-() ]+/g, "_");
    cb(null, safe);
  },
});

const uploadOutput = multer({
  storage: outputStorage,
  limits: { fileSize: 5 * 1024 * 1024 * 1024 }, // 5GB
});

// =====================
// BASIC
// =====================
app.get("/", (_req, res) => res.send("mapxion api ok"));

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});


app.get("/admin/jobs", async (_req, res) => {
  try {
    const { rows } = await pool.query(`
      select
        id,
        client_email,
        project_name,
        client_name,
        status,
        stage,
        photos_count,
        price,
        progress,
        message,
        error,
        created_at,
        started_at,
        finished_at,
        download_seconds,
        processing_seconds,
        total_seconds
        ${jobsHasQualityMode ? ", quality_mode" : ""}
      from jobs
      order by created_at desc
      limit 100
    `);

    res.json({
      ok: true,
      jobs: rows
    });

  } catch (e) {
    console.error("admin jobs error", e);
    res.status(500).json({
      ok: false,
      error: "admin jobs error"
    });
  }
});

app.get("/version", (_req, res) =>
  res.json({ version: "v35-worker-receiving-list" })
);

app.get("/redis", (_req, res) =>
  res.json({ configured: !!process.env.REDIS_URL, ready: redisReady })
);

app.get("/queue", async (_req, res) => {
  try {
    if (!processQueue || !redisReady) {
      return res.status(503).json({ ok: false, error: "queue unavailable" });
    }
    const counts = await processQueue.getJobCounts();
    res.json({ ok: true, counts });
  } catch (e) {
    console.error("queue error", e);
    res.status(500).json({ ok: false, error: "queue error" });
  }
});

// =====================
// PRICING PREVIEW
// =====================
app.post("/pricing/preview", async (req, res) => {
  try {
    console.log("PRICING PREVIEW origin:", req.headers.origin);
    console.log("PRICING PREVIEW body:", req.body);

    const photosCount = Number(req.body?.photos_count || 0);
    const totalBytes = Number(req.body?.total_bytes || 0);
    const qualityMode = normalizeQualityMode(req.body?.quality_mode);

    console.log("PRICING PREVIEW parsed:", { photosCount, totalBytes, qualityMode });

    if (!Number.isFinite(photosCount) || photosCount < 0) {
      return res.status(400).json({ ok: false, error: "invalid photos_count" });
    }

    if (!Number.isFinite(totalBytes) || totalBytes < 0) {
      return res.status(400).json({ ok: false, error: "invalid total_bytes" });
    }

    const estimatedSeconds = await estimateProcessingSecondsFromInputsHistorical(
      pool,
      photosCount,
      totalBytes,
      qualityMode
    );

    console.log("PRICING PREVIEW estimatedSeconds:", estimatedSeconds);

    const price = calculatePriceFromInputs(
      photosCount,
      totalBytes,
      estimatedSeconds,
      qualityMode
    );

    console.log("PRICING PREVIEW price:", price);

    return res.json({
      ok: true,
      photos_count: photosCount,
      total_bytes: totalBytes,
      quality_mode: qualityMode,
      quality_mode_label: getQualityModeLabel(qualityMode),
      price,
      estimated_seconds: estimatedSeconds,
      estimated_human: formatEtaSeconds(estimatedSeconds)
    });
  } catch (e) {
    console.error("pricing preview error", e);
    res.status(500).json({ ok: false, error: "pricing preview error" });
  }
});

// =====================
// AUTH
// =====================
app.post("/auth/register", async (req, res) => {
  try {
    const email = String(req.body?.email || "").trim().toLowerCase();
    const password = String(req.body?.password || "");
    const name = String(req.body?.name || "").trim();

    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      return res.status(400).json({
        ok: false,
        error: "invalid_email",
        message: "Email no válido"
      });
    }

    if (!password || password.length < 6) {
      return res.status(400).json({
        ok: false,
        error: "invalid_password",
        message: "La contraseña debe tener al menos 6 caracteres"
      });
    }

    const existing = await pool.query(
      `select id from users where email = $1 limit 1`,
      [email]
    );

    if (existing.rows.length) {
      return res.status(409).json({
        ok: false,
        error: "email_exists",
        message: "Ese email ya está registrado"
      });
    }

    const passwordHash = await bcrypt.hash(password, 10);

    const { rows } = await pool.query(
      `insert into users (email, password_hash, name)
       values ($1, $2, $3)
       returning id, email, name, created_at`,
      [email, passwordHash, name || null]
    );

    return res.json({
      ok: true,
      user: rows[0]
    });
  } catch (e) {
    console.error("register error", e);
    res.status(500).json({
      ok: false,
      error: "register_error"
    });
  }
});

app.post("/auth/login", async (req, res) => {
  try {
    const email = String(req.body?.email || "").trim().toLowerCase();
    const password = String(req.body?.password || "");

    if (!email || !password) {
      return res.status(400).json({
        ok: false,
        error: "missing_credentials",
        message: "Faltan credenciales"
      });
    }

    const { rows } = await pool.query(
      `select id, email, name, password_hash
       from users
       where email = $1
       limit 1`,
      [email]
    );

    if (!rows.length) {
      return res.status(401).json({
        ok: false,
        error: "invalid_credentials",
        message: "Credenciales incorrectas"
      });
    }

    const user = rows[0];
    const valid = await bcrypt.compare(password, user.password_hash);

    if (!valid) {
      return res.status(401).json({
        ok: false,
        error: "invalid_credentials",
        message: "Credenciales incorrectas"
      });
    }

    return res.json({
      ok: true,
      user: {
        id: user.id,
        email: user.email,
        name: user.name
      }
    });
  } catch (e) {
    console.error("login error", e);
    res.status(500).json({
      ok: false,
      error: "login_error"
    });
  }
});

app.post("/auth/invite-login", async (req, res) => {
  try {
    const email = String(req.body?.email || "").trim().toLowerCase();
    const name = String(req.body?.name || "").trim();
    const code = String(req.body?.code || "").trim().toUpperCase();

    if (!email || !isValidEmail(email)) {
      return res.status(400).json({
        ok: false,
        error: "invalid_email",
        message: "Email no válido"
      });
    }

    if (!name) {
      return res.status(400).json({
        ok: false,
        error: "missing_name",
        message: "Debe indicar nombre o empresa"
      });
    }

    if (!code) {
      return res.status(400).json({
        ok: false,
        error: "missing_code",
        message: "Debe indicar un código"
      });
    }

    const invite = await pool.query(
      `select *
       from invite_codes
       where code = $1
       limit 1`,
      [code]
    );

    if (!invite.rows.length) {
      return res.status(404).json({
        ok: false,
        error: "invalid_code",
        message: "Código no válido"
      });
    }

    const inviteRow = invite.rows[0];

    if (inviteRow.is_used) {
      return res.status(409).json({
        ok: false,
        error: "code_already_used",
        message: "Ese código ya ha sido utilizado"
      });
    }

    let userRow = null;

    const existingUser = await pool.query(
      `select id, email, name
       from users
       where email = $1
       limit 1`,
      [email]
    );

    if (existingUser.rows.length) {
      userRow = existingUser.rows[0];
    } else {
      const guestPasswordHash = await bcrypt.hash(`guest:${code}:${Date.now()}`, 10);

      const createdUser = await pool.query(
        `insert into users (email, password_hash, name)
         values ($1, $2, $3)
         returning id, email, name`,
        [email, guestPasswordHash, name]
      );

      userRow = createdUser.rows[0];
    }

    await pool.query(
      `update invite_codes
          set is_used = true,
              used_at = now(),
              used_by_email = $2,
              used_by_name = $3
        where id = $1`,
      [inviteRow.id, email, name]
    );

    return res.json({
      ok: true,
      user: {
        id: userRow.id,
        email: userRow.email,
        name: userRow.name,
        auth_type: "invite"
      }
    });
  } catch (e) {
    console.error("invite login error", e);
    res.status(500).json({
      ok: false,
      error: "invite_login_error"
    });
  }
});

app.post("/admin/invite-codes/generate", async (req, res) => {
  try {
    const count = Number(req.body?.count || 50);
    const rowsCreated = [];

    for (let i = 0; i < count; i++) {
      let code;
      let inserted = false;

      while (!inserted) {
        code = generateInviteCode();

        try {
          const { rows } = await pool.query(
            `insert into invite_codes (code)
             values ($1)
             returning *`,
            [code]
          );
          rowsCreated.push(rows[0]);
          inserted = true;
        } catch (e) {
          if (!String(e.message || "").toLowerCase().includes("duplicate")) {
            throw e;
          }
        }
      }
    }

    res.json({
      ok: true,
      created: rowsCreated.length,
      codes: rowsCreated
    });
  } catch (e) {
    console.error("generate invite codes error", e);
    res.status(500).json({
      ok: false,
      error: "generate_invite_codes_error"
    });
  }
});

app.get("/admin/invite-codes", async (_req, res) => {
  try {
    const { rows } = await pool.query(
      `select
         id,
         code,
         is_used,
         used_at,
         used_by_email,
         used_by_name,
         created_at
       from invite_codes
       order by created_at desc
       limit 500`
    );

    res.json({
      ok: true,
      codes: rows
    });
  } catch (e) {
    console.error("list invite codes error", e);
    res.status(500).json({
      ok: false,
      error: "list_invite_codes_error"
    });
  }
});
// =====================
// JOBS
// =====================
app.get("/jobs", async (_req, res) => {
  try {
    const { rows } = await pool.query(
      "select * from jobs order by created_at desc limit 50"
    );
    res.json(rows);
  } catch (e) {
    console.error("list jobs error", e);
    res.status(500).json({ error: "db error" });
  }
});

app.get("/jobs/mine", async (req, res) => {
  try {
    const userId = req.headers["x-user-id"];

    if (!userId) {
      return res.status(401).json({
        ok: false,
        error: "missing_user",
        message: "Falta user_id"
      });
    }

    const { rows } = await pool.query(
      `select
        id,
        project_name,
        status,
        stage,
        progress,
        price,
        created_at
      from jobs
      where user_id = $1
      order by created_at desc`,
      [userId]
    );

    res.json({
      ok: true,
      jobs: rows
    });
  } catch (e) {
    console.error("jobs/mine error", e);
    res.status(500).json({
      ok: false,
      error: "jobs_mine_error"
    });
  }
});

app.get("/jobs/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { rows } = await pool.query("select * from jobs where id = $1", [id]);
    if (!rows.length) return res.status(404).json({ error: "not found" });
    res.json(rows[0]);
  } catch (e) {
    console.error("get job error", e);
    res.status(500).json({ error: "db error" });
  }
});

// ✅ OPCION A: crear job (NO ENCOLA AQUI)
// 🔥 v13: ya NO exige photos_count. Crea job “created” con 0 fotos y precio 0.
app.post("/jobs", async (req, res) => {
  try {
   
const exifSummaryRaw = stripNullCharsDeep(req.body?.exif_summary || null);
const outputsRequested = stripNullCharsDeep(req.body?.outputs_requested || []);
const presetKey = stripNullCharsDeep(req.body?.preset_key || null);
const outputMode = stripNullCharsDeep(req.body?.output_mode || null);
const tamsExport = !!req.body?.tams_export;

const exifSummary = stripNullCharsDeep({
  ...(exifSummaryRaw || {}),
  _xproces: {
    // 🔹 conserva lo que venga del frontend (CLAVE)
    ...((exifSummaryRaw && exifSummaryRaw._xproces) || {}),

    // 🔹 siempre actualiza outputs (esto ya lo hacías bien)
    outputs_requested: outputsRequested,

    // 🔹 si viene en body lo usa, si no usa lo del frontend
    preset_key:
      req.body?.preset_key ??
      exifSummaryRaw?._xproces?.preset_key ??
      null,

    output_mode:
      req.body?.output_mode ??
      exifSummaryRaw?._xproces?.output_mode ??
      null,

    tams_export: !!(
      req.body?.tams_export ??
      exifSummaryRaw?._xproces?.tams_export
    )
  }
});

const clientEmail = req.body?.client_email || null;
const projectName = req.body?.project_name || null;
const clientName = req.body?.client_name || null;
const userId = req.body?.user_id || null;
const qualityMode = normalizeQualityMode(req.body?.quality_mode);
    

    if (!clientEmail || !isValidEmail(clientEmail)) {
      return res.status(400).json({
        ok: false,
        error: "invalid_client_email",
        message: "El email del cliente no es válido"
      });
    }

    const insertSql = jobsHasQualityMode
      ? `insert into jobs (
          status,
          photos_count,
          price,
          exif_summary,
          client_email,
          project_name,
          client_name,
          user_id,
          quality_mode
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        returning *`
      : `insert into jobs (
          status,
          photos_count,
          price,
          exif_summary,
          client_email,
          project_name,
          client_name,
          user_id
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8)
        returning *`;

    const insertParams = jobsHasQualityMode
      ? [
          "created",
          0,
          0,
          exifSummary,
          clientEmail,
          projectName,
          clientName,
          userId,
          qualityMode
        ]
      : [
          "created",
          0,
          0,
          exifSummary,
          clientEmail,
          projectName,
          clientName,
          userId
        ];

    const { rows } = await pool.query(insertSql, insertParams);

    const jobRow = rows[0];
    ensureJobDirs(jobRow.id);

    res.json(jobRow);
  } catch (e) {
    console.error("create job error", e);
    res.status(500).json({ error: "db error" });
  }
});

// ✅ OPCION A: submit (encolar cuando ya hay fotos)
// 🔥 v13: calcula inputs reales, calcula precio, y actualiza DB antes de encolar
app.post("/jobs/:id/submit", async (req, res) => {
  try {
    const { id } = req.params;

    const selectJobSql = jobsHasQualityMode
      ? "select id, status, quality_mode from jobs where id = $1"
      : "select id, status from jobs where id = $1";

    const { rows } = await pool.query(selectJobSql, [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    const job = rows[0];
    const qualityMode = normalizeQualityMode(
      req.body?.quality_mode || job.quality_mode
    );

    if (isLockedStatus(job.status)) {
      return res.status(409).json({
        ok: false,
        error: "job_locked",
        message: `Job ya está en estado ${job.status}`,
      });
    }

    if (!processQueue || !redisReady) {
      return res
        .status(503)
        .json({ error: "queue unavailable (redis not ready)" });
    }

    ensureJobDirs(id);

    const inputs = listInputImages(id);

    if (inputs.length === 0) {
      await pool.query(
        `update jobs
           set status='failed',
               progress=0,
               message=$2,
               error=$3,
               updated_at=now(),
               finished_at=now()
         where id=$1`,
        [id, "No hay fotos en input", "no_input_files"]
      );
      return res.status(400).json({
        ok: false,
        error: "no_input_files",
        inputs: 0,
      });
    }

    const photosCount = inputs.length;
    const inputTotalBytes = getInputTotalBytes(id);

    const estimatedSeconds = await estimateProcessingSecondsFromInputsHistorical(
      pool,
      photosCount,
      inputTotalBytes,
      qualityMode
    );

    const price = calculatePriceFromInputs(
      photosCount,
      inputTotalBytes,
      estimatedSeconds,
      qualityMode
    );

    // Guardamos conteo real + precio real y ponemos en cola
    const submitUpdateSql = jobsHasQualityMode
      ? `update jobs
           set status='queued',
               photos_count=$2,
               price=$3,
               input_total_bytes=$4,
               quality_mode=$5,
               progress=0,
               message='En cola',
               error=null,
               updated_at=now(),
               started_at = case when started_at is null then now() else started_at end
         where id=$1`
      : `update jobs
           set status='queued',
               photos_count=$2,
               price=$3,
               input_total_bytes=$4,
               progress=0,
               message='En cola',
               error=null,
               updated_at=now(),
               started_at = case when started_at is null then now() else started_at end
         where id=$1`;

    const submitUpdateParams = jobsHasQualityMode
      ? [id, photosCount, price, inputTotalBytes, qualityMode]
      : [id, photosCount, price, inputTotalBytes];

    await pool.query(submitUpdateSql, submitUpdateParams);

    // encolar
    await processQueue.add(
      "process_job",
      { jobId: id, qualityMode },
      { attempts: 3, backoff: { type: "exponential", delay: 5000 } }
    );

    res.json({
      ok: true,
      enqueued: true,
      jobId: id,
      inputs: photosCount,
      input_total_bytes: inputTotalBytes,
      quality_mode: qualityMode,
      quality_mode_label: getQualityModeLabel(qualityMode),
      price,
      estimated_seconds: estimatedSeconds,
      estimated_human: formatEtaSeconds(estimatedSeconds)
    });
  } catch (e) {
    console.error("submit error", e);
    res.status(500).json({ error: "submit error" });
  }
});

// =====================
// INPUTS (photos)
// =====================

// ✅ acepta "photos" y "photos[]"
app.post("/jobs/:id/upload", uploadInput.any(), async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      "select id, status from jobs where id = $1",
      [id]
    );
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    const job = rows[0];

if (isLockedStatus(job.status)) {
  return res.status(409).json({
    ok: false,
    error: "job_locked",
    message: `No se pueden subir más fotos: estado ${job.status}`,
  });
}

// Si entra la primera foto y el job aún está recién creado,
// lo pasamos a "receiving"
if (job.status === "created") {
  await pool.query(
    `update jobs
        set status='receiving',
            message='Recibiendo fotos',
            updated_at=now()
      where id=$1`,
    [id]
  );
}

ensureJobDirs(id);

    const files = (req.files || [])
      .filter((f) => f.fieldname === "photos" || f.fieldname === "photos[]")
      .map((f) => ({
        field: f.fieldname,
        filename: f.filename,
        size: f.size,
      }));

    if (!files.length) {
      return res.status(400).json({
        ok: false,
        error: "no files uploaded (use photos or photos[])",
      });
    }

    res.json({ ok: true, uploaded: files.length, files });
  } catch (e) {
    console.error("upload error", e);
    res.status(500).json({ ok: false, error: "upload error" });
  }
});

app.get("/jobs/:id/files", async (req, res) => {
  try {
    const { id } = req.params;
    const dir = inputDir(id);

    if (!fs.existsSync(dir)) return res.json({ ok: true, files: [] });

    const files = fs.readdirSync(dir).map((name) => {
      const stat = fs.statSync(path.join(dir, name));
      return { filename: name, size: stat.size };
    });

    res.json({ ok: true, files });
  } catch (e) {
    console.error("files error", e);
    res.status(500).json({ ok: false, error: "files error" });
  }
});

app.get("/jobs/:id/input.zip", async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query("select id from jobs where id = $1", [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    const dir = inputDir(id);
    if (!fs.existsSync(dir))
      return res.status(404).json({ error: "no input folder yet" });

    const files = fs.readdirSync(dir);
    if (!files.length) return res.status(404).json({ error: "no inputs yet" });

    res.setHeader("Content-Type", "application/zip");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="mapxion-${id}-input.zip"`
    );

    const archive = archiver("zip", { zlib: { level: 9 } });

    archive.on("error", (err) => {
      console.error("input.zip error", err);
      try {
        res.status(500).end();
      } catch (_) {}
    });

    req.on("close", () => {
      if (!res.writableEnded) archive.abort();
    });

    archive.pipe(res);
    archive.directory(dir, false);
    await archive.finalize();
  } catch (e) {
    console.error("input.zip error", e);
    res.status(500).json({ error: "input.zip error" });
  }
});

// =====================
// OUTPUTS
// =====================
app.post("/jobs/:id/output", uploadOutput.single("file"), async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query("select id from jobs where id = $1", [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    ensureJobDirs(id);

    if (!req.file)
      return res.status(400).json({ error: "missing file field (file)" });

    res.json({
      ok: true,
      saved: { filename: req.file.filename, size: req.file.size },
    });
  } catch (e) {
    console.error("upload output error", e);
    res.status(500).json({ ok: false, error: "upload output error" });
  }
});

app.get("/jobs/:id/outputs", async (req, res) => {
  try {
    const { id } = req.params;
    const dir = outputDir(id);

    if (!fs.existsSync(dir)) return res.json({ ok: true, files: [] });

    const files = fs.readdirSync(dir).map((name) => {
      const stat = fs.statSync(path.join(dir, name));
      return { filename: name, size: stat.size };
    });

    res.json({ ok: true, files });
  } catch (e) {
    console.error("outputs error", e);
    res.status(500).json({ ok: false, error: "outputs error" });
  }
});

app.get("/jobs/:id/log", async (req, res) => {
  try {
    const { id } = req.params;

    const logPath = path.join(outputDir(id), "metashape-python-log.txt");

    if (!fs.existsSync(logPath)) {
      return res.status(404).json({ ok: false, error: "log not found" });
    }

    const text = fs.readFileSync(logPath, "utf8");

    res.setHeader("Content-Type", "text/plain");
    res.send(text);

  } catch (e) {
    console.error("log error", e);
    res.status(500).json({ ok: false, error: "log error" });
  }
});

app.get("/jobs/:id/log", async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      "select id from jobs where id = $1",
      [id]
    );

    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "job not found" });
    }

    const logPath = path.join(outputDir(id), "metashape-python-log.txt");

    if (!fs.existsSync(logPath)) {
      return res.status(404).json({ ok: false, error: "log not found" });
    }

    const text = fs.readFileSync(logPath, "utf8");

    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    return res.send(text);
  } catch (e) {
    console.error("log error", e);
    return res.status(500).json({ ok: false, error: "log error" });
  }
});

app.get("/jobs/:id/download", async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      "select id from jobs where id = $1",
      [id]
    );

    if (!rows.length)
      return res.status(404).json({ error: "job not found" });

    const zipPath = path.join(outputDir(id), "outputs.zip");

    if (!fs.existsSync(zipPath)) {
      return res.status(404).json({ error: "outputs.zip not found" });
    }

    const stat = fs.statSync(zipPath);

    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Length", stat.size);

    return res.download(zipPath, `xproces-${id}.zip`);

  } catch (e) {
    console.error("download error", e);
    res.status(500).json({ error: "download error" });
  }
});


app.post("/jobs/:id/cancel", async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      `select id, status
       from jobs
       where id = $1`,
      [id]
    );

    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "job not found" });
    }

    const job = rows[0];
    const status = String(job.status || "").toLowerCase();

    if (status === "done") {
      return res.status(409).json({
        ok: false,
        error: "job_already_done",
        message: "No se puede cancelar un trabajo completado"
      });
    }

    if (status === "failed") {
      return res.status(409).json({
        ok: false,
        error: "job_already_failed",
        message: "El trabajo ya está en estado failed"
      });
    }

    if (status === "cancelled") {
      return res.json({
        ok: true,
        alreadyCancelled: true
      });
    }

    const updated = await pool.query(
      `update jobs
          set status = 'cancelled',
              stage = 'failed',
              message = 'Cancelado manualmente desde admin',
              error = 'cancelled_by_admin',
              updated_at = now(),
              finished_at = now()
        where id = $1
        returning *`,
      [id]
    );

    return res.json({
      ok: true,
      job: updated.rows[0]
    });
  } catch (e) {
    console.error("cancel job error", e);
    res.status(500).json({ ok: false, error: "cancel job error" });
  }
});

app.post("/jobs/:id/priority", async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      `update jobs
         set priority = coalesce(priority, 0) + 1,
             updated_at = now()
       where id = $1
       returning *`,
      [id]
    );

    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "job not found" });
    }

    res.json({ ok: true, job: rows[0] });
  } catch (e) {
    console.error("priority error", e);
    res.status(500).json({ ok: false, error: "priority error" });
  }
});

// =====================
// PATCH TRACKING
// =====================
app.patch("/jobs/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const {
      status,
      stage,
      progress,
      message,
      error,
      download_seconds,
      processing_seconds,
      total_seconds
    } = req.body;

    const p = progress === undefined ? null : Number(progress);
    if (p !== null && (!Number.isFinite(p) || p < 0 || p > 100)) {
      return res.status(400).json({ error: "progress must be 0..100" });
    }

    const currentJobResult = await pool.query(
      `select id, status from jobs where id = $1`,
      [id]
    );

    if (!currentJobResult.rows.length) {
      return res.status(404).json({ error: "not found" });
    }

    const currentStatus = String(currentJobResult.rows[0].status || "").toLowerCase();

    if (currentStatus === "cancelled") {
      return res.json({
        ok: true,
        ignored: true,
        message: "Job cancelado; actualización ignorada"
      });
    }
    
    const { rows } = await pool.query(
      `update jobs
         set status = coalesce($2, status),
             stage = coalesce($3, stage),
             progress = coalesce($4, progress),
             message = coalesce($5, message),
             error = coalesce($6, error),
             download_seconds = coalesce($7, download_seconds),
             processing_seconds = coalesce($8, processing_seconds),
             total_seconds = coalesce($9, total_seconds),
             updated_at = now(),
             started_at = case when $2 = 'running' and started_at is null then now() else started_at end,
             finished_at = case when $2 in ('done','failed') then now() else finished_at end
       where id = $1
       returning *`,
      [
        id,
        status ?? null,
        stage ?? null,
        p,
        message ?? null,
        error ?? null,
        download_seconds ?? null,
        processing_seconds ?? null,
        total_seconds ?? null
      ]
    );

    if (!rows.length) return res.status(404).json({ error: "not found" });
    res.json(rows[0]);
  } catch (e) {
    console.error("patch job error", e);
    res.status(500).json({ error: "db error" });
  }
});

// =====================
// WORKER (HTTP por internet)
// =====================
app.post("/worker/claim", requireWorkerAuth, async (_req, res) => {
  try {
    const { rows } = await pool.query(
      `update jobs
          set status='running',
              progress=0,
              message='Worker claimed',
              updated_at=now(),
              started_at=case when started_at is null then now() else started_at end
        where id = (
          select id
          from jobs
          where status='queued'
          order by coalesce(priority, 0) desc, created_at asc
          limit 1
          for update skip locked
        )
        returning *`
    );

    if (!rows.length) {
      return res.json({ ok: true, job: null });
    }

    res.json({ ok: true, job: rows[0] });
  } catch (e) {
    console.error("worker claim error", e);
    res.status(500).json({ ok: false, error: "worker claim error" });
  }
});
app.get("/worker/receiving", requireWorkerAuth, async (_req, res) => {
  try {
    const { rows } = await pool.query(
      `select id, status, photos_count, price, created_at, updated_at, message
       ${jobsHasQualityMode ? ", quality_mode" : ""}
       from jobs
       where status = 'receiving'
       order by created_at asc
       limit 10`
    );

    res.json({ ok: true, jobs: rows });
  } catch (e) {
    console.error("worker receiving error", e);
    res.status(500).json({ ok: false, error: "worker receiving error" });
  }
});
app.get("/worker/jobs/:id/input.zip", requireWorkerAuth, async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query("select id from jobs where id = $1", [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    const dir = inputDir(id);
    if (!fs.existsSync(dir)) {
      return res.status(404).json({ error: "no input folder yet" });
    }

    const files = fs.readdirSync(dir);
    if (!files.length) {
      return res.status(404).json({ error: "no inputs yet" });
    }

    res.setHeader("Content-Type", "application/zip");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="mapxion-${id}-input.zip"`
    );

    const archive = archiver("zip", { zlib: { level: 9 } });

    archive.on("error", (err) => {
      console.error("worker input.zip error", err);
      try {
        res.status(500).end();
      } catch (_) {}
    });

    req.on("close", () => {
      if (!res.writableEnded) archive.abort();
    });

    archive.pipe(res);
    archive.directory(dir, false);
    await archive.finalize();
  } catch (e) {
    console.error("worker input.zip error", e);
    res.status(500).json({ error: "worker input.zip error" });
  }
});

app.post("/worker/jobs/:id/confirm-download", requireWorkerAuth, async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      "select id, input_purged from jobs where id = $1",
      [id]
    );
    if (!rows.length) return res.status(404).json({ ok: false, error: "job not found" });

    const job = rows[0];

    if (job.input_purged) {
      return res.json({ ok: true, alreadyPurged: true });
    }

    const dir = inputDir(id);
    if (fs.existsSync(dir)) {
      fs.rmSync(dir, { recursive: true, force: true });
    }

    await pool.query(
     `update jobs
         set input_purged = true,
             input_purged_at = now(),
             updated_at = now()
       where id = $1`,
     [id]
   );

    res.json({ ok: true, purged: true });
  } catch (e) {
    console.error("confirm-download error", e);
    res.status(500).json({ ok: false, error: "confirm-download error" });
  }
});
app.get("/worker/jobs/:id/files", requireWorkerAuth, async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      "select id from jobs where id = $1",
      [id]
    );
    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "job not found" });
    }

    const dir = inputDir(id);
    if (!fs.existsSync(dir)) {
      return res.json({ ok: true, files: [] });
    }

    const files = fs.readdirSync(dir).map((name) => {
      const stat = fs.statSync(path.join(dir, name));
      return {
        filename: name,
        size: stat.size,
      };
    });

    res.json({ ok: true, files });
  } catch (e) {
    console.error("worker files error", e);
    res.status(500).json({ ok: false, error: "worker files error" });
  }
});
app.get("/worker/jobs/:id/file/:name", requireWorkerAuth, async (req, res) => {
  try {
    const { id, name } = req.params;

    const { rows } = await pool.query(
      "select id from jobs where id = $1",
      [id]
    );
    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "job not found" });
    }

    const safeName = path.basename(name);
    const filePath = path.join(inputDir(id), safeName);

    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ ok: false, error: "file not found" });
    }

    return res.sendFile(filePath);
  } catch (e) {
    console.error("worker file download error", e);
    res.status(500).json({ ok: false, error: "worker file download error" });
  }
});

function estimateProcessingSecondsFromFallback(job) {
  const photos = Number(job.photos_count || 0);
  const bytes = Number(job.input_total_bytes || 0);
  const gb = bytes / (1024 * 1024 * 1024);
  const qualityMode = normalizeQualityMode(job?.quality_mode);

  return Math.round(
    (120 + (photos * 18) + (gb * 420)) * getQualityModeTimeFactor(qualityMode)
  );
}

async function estimateProcessingSecondsFromInputsHistorical(pool, photosCount, totalBytes, qualityMode = "normal") {
  const photos = Number(photosCount || 0);
  const bytes = Number(totalBytes || 0);
  const normalizedQualityMode = normalizeQualityMode(qualityMode);

  const minPhotos = Math.max(0, photos - 200);
  const maxPhotos = photos + 200;

  const minBytes = Math.max(0, Math.round(bytes * 0.5));
  const maxBytes = Math.round(bytes * 1.5);

  const historicalSql = jobsHasQualityMode
    ? `select processing_seconds
       from jobs
       where status = 'done'
         and processing_seconds is not null
         and quality_mode = $5
         and photos_count between $1 and $2
         and input_total_bytes between $3 and $4
       order by created_at desc
       limit 20`
    : `select processing_seconds
       from jobs
       where status = 'done'
         and processing_seconds is not null
         and photos_count between $1 and $2
         and input_total_bytes between $3 and $4
       order by created_at desc
       limit 20`;

  const historicalParams = jobsHasQualityMode
    ? [minPhotos, maxPhotos, minBytes, maxBytes, normalizedQualityMode]
    : [minPhotos, maxPhotos, minBytes, maxBytes];

  const { rows } = await pool.query(historicalSql, historicalParams);

  if (rows.length >= 3) {
    const avg =
      rows.reduce((acc, r) => acc + Number(r.processing_seconds || 0), 0) / rows.length;
    return Math.round(avg);
  }

  return estimateProcessingSecondsFromInputs(photos, bytes, normalizedQualityMode);
}

async function estimateProcessingSeconds(pool, job) {
  if (!job) return 0;

  const photos = Number(job.photos_count || 0);
  const bytes = Number(job.input_total_bytes || 0);
  const qualityMode = normalizeQualityMode(job?.quality_mode);

  if (photos > 0 || bytes > 0) {
    return await estimateProcessingSecondsFromInputsHistorical(
      pool,
      photos,
      bytes,
      qualityMode
    );
  }

  return estimateProcessingSecondsFromFallback(job);
}

function formatEtaSeconds(seconds) {
  const s = Math.max(0, Math.round(seconds));
  const h = Math.floor(s / 3600);
  const m = Math.ceil((s % 3600) / 60);

  if (h <= 0) return `${m} min`;
  return `${h} h ${m} min`;
}

app.get("/jobs/:id/eta", async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      `select id, status, progress, photos_count, input_total_bytes, created_at
       ${jobsHasQualityMode ? ", quality_mode" : ""}
       from jobs
       where id = $1`,
      [id]
    );

    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "job not found" });
    }

    const targetJob = rows[0];

    const allRows = await pool.query(
      `select id, status, progress, photos_count, input_total_bytes, created_at
       ${jobsHasQualityMode ? ", quality_mode" : ""}
       from jobs
       where status in ('queued', 'running')
       order by created_at asc`
    );

    const activeJobs = allRows.rows;

    let waitSeconds = 0;

    for (const j of activeJobs) {
      if (j.id === id) break;

      const est = await estimateProcessingSeconds(pool, j);

      if (j.status === "running") {
        const progress = Math.max(0, Math.min(100, Number(j.progress || 0)));
        const remaining = Math.round(est * (1 - progress / 100));
        waitSeconds += remaining;
      } else if (j.status === "queued") {
        waitSeconds += est;
      }
    }

    const ownProcessingSeconds = await estimateProcessingSeconds(pool, targetJob);
    const totalSeconds = waitSeconds + ownProcessingSeconds;

    res.json({
      ok: true,
      job_id: id,
      status: targetJob.status,
      quality_mode: normalizeQualityMode(targetJob.quality_mode),
      quality_mode_label: getQualityModeLabel(targetJob.quality_mode),
      queue_wait_seconds: waitSeconds,
      own_processing_seconds: ownProcessingSeconds,
      total_estimated_seconds: totalSeconds,
      queue_wait_human: formatEtaSeconds(waitSeconds),
      own_processing_human: formatEtaSeconds(ownProcessingSeconds),
      total_estimated_human: formatEtaSeconds(totalSeconds)
    });
  } catch (e) {
    console.error("eta error", e);
    res.status(500).json({ ok: false, error: "eta error" });
  }
});

// =====================
// LISTEN
// =====================
const port = Number(process.env.PORT) || 3000;
app.listen(port, "0.0.0.0", () => {
  console.log(`mapxion api listening on ${port}`);
});



