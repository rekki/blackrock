package main

import (
	"time"

	"github.com/jessevdk/go-assets"
)

var _Assetsa4bcda6bbb5a0cc2f23aee473af50da405293607 = "<div style=\"width:80%;\">\n  <canvas id=\"canvas\"></canvas>\n</div>\n<hr>\n<div style=\"width:80%;\">\n  <canvas id=\"canvas-uniq\"></canvas>\n</div>\n<hr>\n<div style=\"width:80%;\">\n  <canvas id=\"canvas-avg\"></canvas>\n</div>\n\n<script>\n\nvar data = {{ json .Chart.Points }};\nlet options = {\n  responsive: true,\n  tooltips: {\n    mode: 'index',\n    intersect: false,\n  },\n  hover: {\n    mode: 'nearest',\n    intersect: true\n  },\n  scales: {\n    xAxes: [{\n      display: true,\n    }],\n    yAxes: [{\n      display: true,\n    }]\n  }  \n}\n\nlet countConfig = {\n  type: 'line',\n  data: {\n    labels: [],\n    __labels: {},\n    datasets: [],\n    __datasets: {}\n  },\n  options\n}\n\nlet countUniqueConfig = {\n  type: 'line',\n  data: {\n    labels: [],\n    __labels: {},\n    datasets: [],\n    __datasets: {}\n\n  },\n  options\n}\n\nlet countAvgConfig = {\n  type: 'line',\n  data: {\n    labels: [],\n    __labels: {},\n    datasets: [],\n    __datasets: {}\n  },\n  options  \n}\n\nvar dateOptions = { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' };\nvar hashCode = function(s) {\n  var h = 0, l = s.length, i = 0;\n  if ( l > 0 )\n    while (i < l)\n      h = (h << 5) - h + s.charCodeAt(i++) | 0;\n  return h;\n};\n\nfunction stringToColour(input) {\n  return randomColor({\n    luminosity: 'light',\n    hue: 'monochrome',\n    seed: hashCode(input),\n    format: 'rgba',\n    alpha: 0.5 \n  });\n}\n\nvar add = function(where,bucket_ns, count, k, orig) {\n  let label = new Date(bucket_ns / 1000000).toLocaleDateString(\"en-GB\")\n  let l = where.__labels[bucket_ns]\n  if (!l) {\n    where.labels.push(label)\n    where.__labels[bucket_ns] = true\n  }\n  \n  s = where.__datasets[k]\n  if (!s) {\n    s = {\n      label: k,\n      data: [],\n      backgroundColor: stringToColour(orig),\n    }\n    where.datasets.push(s)\n    where.__datasets[k] = s\n  }\n  s.data.push(count)\n\n}\n\nlet thresh = 2;\nlet topFilter = {}\n{\n  let topKeys = {}\n  for (let d of data) {\n    topKeys[d.event_type] += d.count_unique\n  }\n\n\n\n\n  let topSorted = Object.keys(topKeys)\n  topSorted.sort((a,b) => top[a] - top[b])\n\n  let j = 0\n  for (let x of topSorted) {\n    topFilter[x] = true\n    j++\n    if (j >= thresh)\n      break;\n  }\n}\n\nfor (let d of data) {\n  if (!topFilter[d.event_type]) continue;\n\n  add(countConfig.data, d.bucket_ns, d.count, d.event_type, d.event_type)\n  add(countUniqueConfig.data, d.bucket_ns, d.count_unique, d.event_type + '_uniq', d.event_type)\n  add(countAvgConfig.data, d.bucket_ns, d.count / d.count_unique, d.event_type + '_avg', d.event_type)\n}\n\n\nwindow.countConfig = new Chart(document.getElementById('canvas').getContext('2d'), countConfig);\nwindow.countUniqConfig = new Chart(document.getElementById('canvas-uniq').getContext('2d'), countUniqueConfig);\nwindow.countAvgConfig = new Chart(document.getElementById('canvas-avg').getContext('2d'), countAvgConfig);\n\nconsole.log(data)\n</script>"
var _Assets543aa4220323067c7b7aa789a3842050314b3ba3 = "<!DOCTYPE html>\n<html>\n  <head>\n    <title>\n      blackrock\n    </title>\n    <style>\n      @font-face {\n      font-family: blackrock;\n      src: url('/external/vendor/font/hack.woff') format('woff'),\n           url('/external/vendor/font/hack.woff2') format('woff2');\n      }\n\n      body {\n      margin: 5px;\n      font-family: \"blackrock\", monospace;\n      background-color: white;\n      color: #3f3f3f;\n      max-width: 60rem;\n      padding: 1rem;\n      font-size: 14px;\n      margin: auto;\n      }\n      .trigger input[type=checkbox] + span {\n      visibility: hidden;\n      display: none;\n      opacity: 0;\n      }\n      .trigger input[type=checkbox]:checked + span {\n      visibility: visible;\n      display: block;\n      opacity: 1;\n      transition-delay: 0s;\n      }\n      .hit-details {\n      display: none;\n      }\n      .msg {\n      display: block;\n      }\n      pre {\n      font-family: \"blackrock\", Monaco, monospace;\n      }\n      a {\n      color: #000;\n      text-decoration: none;\n      }\n      .hit-sep {\n        background-color: gray;\n      }\n      .left {\n      width: 30%;\n      word-break: break-word\n      }\n      .right {\n      width: 30%;\n      word-break: break-word\n      }\n      table {\n      width: 100%;\n      }\n      .banner {\n      background-color: gray;\n      color: white;\n      }\n    </style>\n\n    <link rel=\"stylesheet\" href=\"/external/vendor/chart/Chart.min.css\"/>\n    <script src=\"/external/vendor/chart/Chart.min.js\"></script>\n    <script src=\"/external/vendor/chart/randomColor.js\"></script>\n\n    <script>\n  var toggle = function(id) {\n    var elements = document.getElementsByClassName('hit-' + id)\n    if (elements)\n      for (var i = 0; i < elements.length; i++) \n        elements[i].style.display = \"table-row\"\n\n  }\n    </script>\n  </head>\n  <body>\n    {{ $base := .BaseUrl }}\n    {{ $query := .QueryString }}\n    {{ $total := .Stats.TotalCount }}\n    <div style=\"line-height: 25px;\">\n      <a href=\"{{ replace $base \"/html/\" \"/text/\" }}\">text</a>\n      <br>\n      <a href=\"/scan/html/?{{$query}}\">/scan/html/</a><br>\n      {{ $crumbs := .Crumbs }}\n      {{ range .Crumbs }}\n      <a href=\"{{ .Base }}/{{.Exact}}?{{$query}}\">{{.Exact}}</a> <a href=\"/scan/html/{{.Exact}}?{{$query}}\">=</a> <a href=\"{{ removeQuery $crumbs .Exact}}?{{$query}}\">d</a>{{ if gt (len $crumbs) 1 }} <a href=\"{{ negateQuery $crumbs .Exact}}?{{$query}}\">-</a> {{end}}\n      <br>\n      {{ end }}\n      <form method=get action='?'>\n        <input type=hidden name='query_max_documents' value='{{getS $query \"query_max_documents\"}}'>\n        <input name=\"from\" type=date value='{{getS $query \"from\"}}'>\n        <input name=\"to\" type=date value='{{getS $query \"to\"}}'>\n        <select name=\"bucket\">\n          {{ $selected := getS $query \"bucket\" }}\n          <option  {{ if eq $selected \"minute\"}}selected{{end}}>minute</option>\n          <option  {{ if eq $selected \"hour\"}}selected{{end}}>hour</option>\n          <option  {{ if eq $selected \"day\"}}selected{{end}}>day</option>\n          <option  {{ if eq $selected \"week\"}}selected{{end}}>week</option>\n        </select>\n\n        <input type=submit value=go>\n      </form>\n      total events: {{ $total }}\n{{ if .Stats.ConvertedCache }}\n  <br>\n  {{ $totalConvertions := .Stats.TotalCountEventsFromConverter }}\n  {{ $perVariant := .Stats.ConvertedCache.TotalConvertingUsers }}\n  {{ $confidence := .Stats.ConvertedCache.Confidence $perVariant }}\n  <div>confidence: {{formatFloat $confidence.Confidence}}% significant: {{ $confidence.Significant }}</div>\n{{ range $variant, $value := $perVariant }}\n  <div style=\"background-color: {{ variantColor $variant}}\">v{{ $variant }} converting users: {{ $value.ConvertingUsers}}, non converting users: {{ $value.NotConvertingUsers}}, convertions: {{ $value.Convertions}}, convertion rate: {{ percent $value.Users $value.ConvertingUsers}}% </div>\n{{ end }}\n  <br>\n{{ end }}\n\n    </div>\n\n    <table border=0>\n      <tr><td colspan=2><pre class=\"banner\">{{ banner \"basic\"}}</pre></td></tr>\n      {{ range pick .Stats.Search \"env\" \"product\" \"year-month-day\" }}\n      {{ template \"/html/t/chart.tmpl\" dict \"Data\" . \"GlobalTotal\" $total \"BaseUrl\" $base \"Link\" true \"QueryString\" $query}}\n      {{ end }}\n\n      {{ if .Stats.Chart }}\n      {{ template \"/html/t/graph.tmpl\" .Stats }}\n      {{ end }}\n      <tr><td colspan=2><pre class=\"banner\">{{ banner \"foreign id\"}}</pre></td></tr>\n      {{ range (.Stats.SortedKeys .Stats.Foreign) }}\n      {{ template \"/html/t/chart.tmpl\" dict \"Data\" . \"GlobalTotal\" $total \"BaseUrl\" $base \"Link\" true \"QueryString\" $query \"Foreign\" true}}\n      {{ end }}\n      <tr><td colspan=2><pre class=\"banner\">{{ banner \"event type\"}}</pre></td></tr>\n      {{ template \"/html/t/chart.tmpl\" dict \"Data\" .Stats.EventTypes \"GlobalTotal\" $total  \"BaseUrl\" $base \"Link\" true \"QueryString\" $query}}\n\n      <tr><td colspan=2><pre class=\"banner\">{{ banner \"search\"}}</pre></td></tr>\n      {{ range (.Stats.SortedKeys .Stats.Search) }}\n      {{ template \"/html/t/chart.tmpl\" dict \"Data\" . \"GlobalTotal\" $total  \"BaseUrl\" $base \"Link\" true \"QueryString\" $query}}\n      {{ end }}\n\n      <tr><td colspan=2><pre class=\"banner\">{{ banner \"count\"}}</pre></td></tr>\n      {{ range (.Stats.SortedKeys .Stats.Count) }}\n      {{ template \"/html/t/chart.tmpl\" dict \"Data\" . \"GlobalTotal\" $total  \"BaseUrl\" $base \"Link\" false \"QueryString\" $query}}\n      {{ end }}\n\n      <tr><td colspan=2><pre id=\"sample\" class=\"banner\">{{ banner \"sample\"}}</pre></td></tr>\n      {{ range $variant,$hits := .Stats.Sample }}\n      {{ range $hits }}\n      {{ template \"/html/t/hit.tmpl\" dict \"Data\" . \"QueryString\" $query \"BaseUrl\" $base \"Variant\" $variant}}\n      {{ end  }}\n      {{ end  }}\n    </table>\n  </body>\n</html>"
var _Assetsd675bab369b2bbedd927d7a44809bf92a4458c2f = "{{ $hit := .Data }}\n{{ $qs := .QueryString }}\n{{ $base := .BaseUrl }}\n{{ $variant := .Variant }}\n{{ $m := $hit.Metadata }}\n{{ $foreignId := $m.ForeignId }}\n{{ $foreignType := $m.ForeignType }}\n<tr id=\"#{{$hit.ID}}\" class=\"hit-header\">\n  <td>\n    <a style=\"font-size: 18px\" href=\"/scan/html/event_type:{{ $m.EventType }}?{{$qs}}\">{{$m.EventType }}</a><br>\n    <a href=\"/scan/html/{{$m.ForeignType}}:{{ $m.ForeignId }}?{{$qs}}\">{{$m.ForeignType}}:{{$m.ForeignId}}</a><br>\n    {{ time $m.CreatedAtNs }}, v{{$variant}}\n    <br>\n  </td>\n  <td align=\"right\">\n <a href=\"javascript: toggle({{$hit.ID}})\">show</a>\n  </td>\n</tr>\n{{ range $m.Search }}\n<tr class=\"hit-details hit-{{$hit.ID}}\">\n  <td class=\"left\">\n    {{ .Key }}\n  </td>\n  <td class=\"right\">\n    <a href=\"/scan/html/{{ $foreignType }}:{{ $foreignId }}/{{ .Key }}:{{ .Value}}?{{$qs}}\">{{.Value}}</a>\n  </td>\n</tr>\n{{ end }}\n\n{{ range $m.Count }}\n<tr class=\"hit-details hit-{{$hit.ID}}\">\n  <td class=\"left\">\n      {{ .Key }}\n  </td>\n  <td class=\"right\">\n      {{.Value}}\n  </td>\n</tr>\n{{ end }}\n\n\n{{ range $m.Properties }}\n<tr class=\"hit-details hit-{{$hit.ID}}\">\n  <td class=\"left\">\n      {{ .Key }}\n  </td>\n  <td class=\"right\">\n      {{.Value}}\n  </td>\n</tr>\n{{ end }}\n\n  {{ range $hit.Context }}\n    <tr class=\"hit-details hit-{{$hit.ID}}\">\n      <td class=\"left\" colspan=2>\n        @{{.ForeignType}}:{{ .ForeignId }}\n      </td>\n    </tr>\n    {{ range .Properties }}\n    <tr class=\"hit-details hit-{{$hit.ID}}\">\n      <td class=\"left\">\n        &nbsp;&nbsp;{{.Key }}\n      </td>\n      <td class=\"right\">\n        {{.Value}}\n      </td>\n    </tr>\n    {{ end }}\n  {{ end }}\n<tr>\n<td colspan=2><hr></td>\n</tr>"
var _Assets6a465482d807e30a440fbe28a869d530372e9986 = "{{ $sorted := .Data.Sorted }}\n<tr id=\"{{ .Data.Key }}\">\n  <td class=\"left\">\n    {{ .Data.Key }} ({{ (len $sorted) }})\n  </td>\n  <td class=\"right\">\n    total: {{ .Data.Count }} {{ percent .GlobalTotal .Data.Count }}\n  </td>\n</tr>\n\n{{ $total := .Data.Count }}\n{{ $totalConvertions := .Data.CountEventsFromConverter }}\n{{ $b := .BaseUrl }}\n{{ $qs := .QueryString }}\n{{ $tag := .Data.Key }}\n{{ $link := .Link }}\n{{ $foreign := .Foreign }}\n{{ $limit := getN $qs .Data.Key 50 }}\n{{ range $i, $val := $sorted }}\n\n{{ if lt $i $limit}}\n<tr>\n  <td class=\"left\">\n    {{ if $link }}\n    <div style=\"padding-left: 1.5em;\">\n    {{ $p := ctx $tag $val.Key }}\n    {{ $name := findFirstNameOrDefault $p $val.Key }}\n    {{ if $p }}\n    <label class=\"trigger\">\n      <input type=\"checkbox\"/>\n      <span class=\"msg\">\n        <table>\n          <tr><td colspan=2><b><a href=\"{{$b}}/{{$tag}}:{{$val.Key}}?{{$qs}}\">{{ $name }}</a></b></td></tr>\n          {{ range $p }}\n          <tr><td><b>{{.ForeignType}}</b></td><td>{{.ForeignId}}</td></tr>\n          {{ range .Properties }}\n          <tr><td>{{prettyFlat .Key}}</td><td>{{ .Value}}</td></tr>\n          {{ end }}\n          <tr><td colspan=1><hr /></td></tr>\n          {{ end }}\n        </table>\n      </span>\n    </label>\n    {{ end }}\n    <a href=\"{{$b}}/{{$tag}}:{{$val.Key}}?{{$qs}}\">{{ $name }}</a>\n    </div>\n    {{ else }}\n    <div style=\"padding-left: 1.5em\">{{ $val.Key }}</div>\n    {{ end }}\n  </td>\n  <td class=\"right\">\n    {{ $p := percent $total $val.Count}}\n    <div style=\"position: relative\">\n     <div style=\"display: inline-block; z-index: 1\">{{ format $val.Count }} ({{$p}}%)</div>\n     <div style=\"position: absolute; left: 0; bottom: 0;  display: inline-block; width: {{$p}}%; background-color: rgba(200,200,200,0.5);\">&nbsp;</div>\n    </div>\n    {{ range $variant, $count := $val.CountEventsFromConverterVariant }}\n\n    {{ if gt $count 0 }}\n    {{ $p := percent $totalConvertions $count}}\n    <div style=\"position: relative\">\n     <div style=\"display: inline-block; z-index: 1\">v{{ $variant }} {{ format $count }} ({{$p}}%)</div>\n     {{ $color := variantColor $variant }}\n     <div style=\"position: absolute; left: 0; bottom: 0;  display: inline-block; width: {{$p}}%; background-color: {{$color}};\">&nbsp;</div>\n    </div>\n    {{ end }}\n    {{ end }}\n  </td>\n</tr>\n\n{{ end }}\n{{ end }}\n\n{{ if gt (len $sorted) $limit }}\n<tr>\n  <td class=\"left\" colspan=2>\n    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;...<a href=\"{{$b}}?{{addN $qs .Data.Key 100}}#{{.Data.Key}}\">skipping {{ minus (len $sorted) $limit }}</a>\n  </td>\n</tr>\n\n{{end}}\n<tr>\n  <td>&nbsp;</td>\n</tr>"

// Assets returns go-assets FileSystem
var Assets = assets.NewFileSystem(map[string][]string{"/html/t": []string{"hit.tmpl", "chart.tmpl", "graph.tmpl", "index.tmpl"}, "/": []string{"html"}, "/html": []string{}}, map[string]*assets.File{
	"/html/t/index.tmpl": &assets.File{
		Path:     "/html/t/index.tmpl",
		FileMode: 0x1b4,
		Mtime:    time.Unix(1566911631, 1566911631256445673),
		Data:     []byte(_Assets543aa4220323067c7b7aa789a3842050314b3ba3),
	}, "/": &assets.File{
		Path:     "/",
		FileMode: 0x800001fd,
		Mtime:    time.Unix(1566913411, 1566913411413128494),
		Data:     nil,
	}, "/html": &assets.File{
		Path:     "/html",
		FileMode: 0x800001fd,
		Mtime:    time.Unix(1566911642, 1566911642468515461),
		Data:     nil,
	}, "/html/t": &assets.File{
		Path:     "/html/t",
		FileMode: 0x800001fd,
		Mtime:    time.Unix(1566914672, 1566914672358164982),
		Data:     nil,
	}, "/html/t/hit.tmpl": &assets.File{
		Path:     "/html/t/hit.tmpl",
		FileMode: 0x1b4,
		Mtime:    time.Unix(1566165739, 1566165739995362109),
		Data:     []byte(_Assetsd675bab369b2bbedd927d7a44809bf92a4458c2f),
	}, "/html/t/chart.tmpl": &assets.File{
		Path:     "/html/t/chart.tmpl",
		FileMode: 0x1b4,
		Mtime:    time.Unix(1566165828, 1566165828691866938),
		Data:     []byte(_Assets6a465482d807e30a440fbe28a869d530372e9986),
	}, "/html/t/graph.tmpl": &assets.File{
		Path:     "/html/t/graph.tmpl",
		FileMode: 0x1b4,
		Mtime:    time.Unix(1566914672, 1566914672346164898),
		Data:     []byte(_Assetsa4bcda6bbb5a0cc2f23aee473af50da405293607),
	}}, "")
