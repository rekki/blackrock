package main

import (
	"time"

	"github.com/jessevdk/go-assets"
)

var _Assetsd675bab369b2bbedd927d7a44809bf92a4458c2f = "{{ $hit := .Data }}\n{{ $qs := .QueryString }}\n{{ $base := .BaseUrl }}\n{{ $m := $hit.Metadata }}\n{{ $foreignId := $m.ForeignId }}\n{{ $foreignType := $m.ForeignType }}\n<tr id=\"#{{$hit.Id}}\" class=\"hit-header\">\n  <td>\n    <a style=\"font-size: 18px\" href=\"/query/event_type:{{ $m.EventType }}?{{$qs}}\">{{$m.EventType }}</a><br>\n    <a href=\"/query/{{$m.ForeignType}}:{{ $m.ForeignId }}?{{$qs}}\">{{$m.ForeignType}}:{{$m.ForeignId}}</a><br>\n    {{ time $m.CreatedAtNs }}\n    <br>\n  </td>\n  <td align=\"right\">\n <a href=\"javascript: toggle('{{$hit.Id}}')\">show</a>\n  </td>\n</tr>\n{{ range $m.Search }}\n<tr class=\"hit-details hit-{{$hit.Id}}\">\n  <td class=\"left\">\n    {{ .Key }}\n  </td>\n  <td class=\"right\">\n    {{ if eq .Key \"ip\" }}\n    [ hidden ]\n    {{ else }}\n    <a href=\"/query/{{ $foreignType }}:{{ $foreignId }}/{{ .Key }}:{{ .Value}}?{{$qs}}\">{{.Value}}</a>\n    {{ end }}\n  </td>\n</tr>\n{{ end }}\n\n{{ range $m.Count }}\n<tr class=\"hit-details hit-{{$hit.Id}}\">\n  <td class=\"left\">\n      {{ .Key }}\n  </td>\n  <td class=\"right\">\n      {{.Value}}\n  </td>\n</tr>\n{{ end }}\n\n\n{{ range $m.Properties }}\n<tr class=\"hit-details hit-{{$hit.Id}}\">\n  <td class=\"left\">\n      {{ .Key }}\n  </td>\n  <td class=\"right\">\n      {{.Value}}\n  </td>\n</tr>\n{{ end }}\n<tr>\n<td colspan=2><hr></td>\n</tr>"
var _Assets6a465482d807e30a440fbe28a869d530372e9986 = "{{ $qs := .QueryString }}\n{{ $sorted := sortedKV .Data }}\n{{ $b := .BaseUrl }}\n\n{{ $link := .Link }}\n\n{{ range $skv := $sorted }}\n\n<tr>\n  <td class=\"left\">\n      {{ $skv.Key }}\n  </td>\n  <td class=\"right\">\n  </td>\n</tr>\n\n{{ range $counted := sortedCount $skv.Count}}\n<tr>\n  <td class=\"left\">\n    {{ if $link }}\n    <div style=\"padding-left: 1.5em;\">\n    {{ $name := prettyName $skv.Key $counted.Key }}\n    <a href=\"{{$b}}/{{$skv.Key}}:{{$counted.Key}}?{{$qs}}\">{{ $name }}</a>\n    </div>\n    {{ else }}\n    <div style=\"padding-left: 1.5em\">{{ $counted.Key }}</div>\n    {{ end }}\n  </td>\n  <td class=\"right\">\n    {{ $p := percent $skv.Total $counted.Count}}\n    <div style=\"position: relative\">\n     <div style=\"display: inline-block; z-index: 1\">{{ format $counted.Count }} ({{$p}}%)</div>\n     <div style=\"position: absolute; left: 0; bottom: 0;  display: inline-block; width: {{$p}}%; background-color: rgba(200,200,200,0.5);\">&nbsp;</div>\n    </div>\n  </td>\n</tr>\n{{ end }}\n\n<tr>\n  <td>&nbsp;</td>\n</tr>\n{{end}}"
var _Assets543aa4220323067c7b7aa789a3842050314b3ba3 = "<!DOCTYPE html>\n<html>\n  <head>\n    <title>\n      blackrock\n    </title>\n    <style>\n      @font-face {\n      font-family: blackrock;\n      src: url('/external/vendor/font/hack.woff') format('woff'),\n           url('/external/vendor/font/hack.woff2') format('woff2');\n      }\n\n      body {\n      margin: 5px;\n      font-family: \"blackrock\", monospace;\n      background-color: white;\n      color: #3f3f3f;\n      max-width: 60rem;\n      padding: 1rem;\n      font-size: 14px;\n      margin: auto;\n      }\n      .hit-details {\n      display: none;\n      }\n      .msg {\n      display: block;\n      }\n      pre {\n      font-family: \"blackrock\", Monaco, monospace;\n      }\n      a {\n      color: #000;\n      text-decoration: none;\n      }\n      .hit-sep {\n        background-color: gray;\n      }\n      .left {\n      width: 30%;\n      word-break: break-word\n      }\n      .right {\n      width: 30%;\n      word-break: break-word\n      }\n      table {\n      width: 100%;\n      }\n      .banner {\n      background-color: gray;\n      color: white;\n      }\n    </style>\n\n    <link rel=\"stylesheet\" href=\"/external/vendor/chart/Chart.min.css\"/>\n    <script src=\"/external/vendor/chart/Chart.min.js\"></script>\n    <script src=\"/external/vendor/chart/randomColor.js\"></script>\n    <script>\n  var toggle = function(id) {\n    var elements = document.getElementsByClassName('hit-' + id)\n    if (elements)\n      for (var i = 0; i < elements.length; i++) \n        elements[i].style.display = \"table-row\"\n\n  }\n    </script>\n\n  </head>\n  <body>\n    {{ $base := .BaseUrl }}\n    {{ $query := .QueryString }}\n    {{ $total := .Agg.Total }}\n    {{ $agg := .Agg }}\n    <div style=\"line-height: 25px;\">\n      <a href=\"/query/?{{$query}}\">/query/</a><br>\n      {{ $crumbs := .Crumbs }}\n      {{ range .Crumbs }}\n      <a href=\"{{ .Base }}/{{.Exact}}?{{$query}}\">{{.Exact}}</a> <a href=\"/query/{{.Exact}}?{{$query}}\">=</a> <a href=\"{{ $crumbs.RemoveQuery .Exact}}?{{$query}}\">d</a>{{ if gt (len $crumbs) 1 }} <a href=\"{{ $crumbs.NegateQuery .Exact}}?{{$query}}\">-</a> {{end}}\n      <br>\n      {{end}}\n\n      <form method=get action='?'>\n        <input type=hidden name='query_max_documents' value='{{getS $query \"query_max_documents\"}}'>\n        <input name=\"from\" type=date value='{{getS $query \"from\"}}'>\n        <input name=\"to\" type=date value='{{getS $query \"to\"}}'>\n        <select name=\"bucket\">\n          {{ $selected := getS $query \"bucket\" }}\n          <option  {{ if eq $selected \"minute\"}}selected{{end}}>minute</option>\n          <option  {{ if eq $selected \"hour\"}}selected{{end}}>hour</option>\n          <option  {{ if eq $selected \"day\"}}selected{{end}}>day</option>\n          <option  {{ if eq $selected \"week\"}}selected{{end}}>week</option>\n        </select>\n        <input type=submit value=go><br>\n        <select name=\"whitelist\" multiple style=\"width: 50%\" size=10>\n          <option disabled></option>\n          {{ range .SortedSections }}\n          <option value=\"{{.Key}}\" {{ if .Selected }}selected{{end}}>{{.Key}} ({{.Count}})</option>\n          {{ end }}\n        </select>\n      </form>\n      total events: {{ $total }}\n    </div>\n\n    <table border=0>\n      <tr><td colspan=2><pre class=\"banner\">{{ banner \"event_type\"}}</pre></td></tr>\n      {{ template \"/html/t/chart.tmpl\" dict \"Data\" .Agg.EventType \"QueryString\" $query \"BaseUrl\" $base \"GlobalTotal\" $total \"Link\" true}}\n\n      <tr><td colspan=2><pre class=\"banner\">{{ banner \"foreign_id\"}}</pre></td></tr>\n      {{ template \"/html/t/chart.tmpl\" dict \"Data\" .Agg.ForeignId \"QueryString\" $query \"BaseUrl\" $base \"GlobalTotal\" $total \"Link\" true}}\n\n      <tr><td colspan=2><pre class=\"banner\">{{ banner \"search\"}}</pre></td></tr>\n      {{ template \"/html/t/chart.tmpl\" dict \"Data\" .Agg.Search \"QueryString\" $query \"BaseUrl\" $base \"GlobalTotal\" $total \"Link\" true}}\n\n      <tr><td colspan=2><pre class=\"banner\">{{ banner \"count\"}}</pre></td></tr>\n      {{ template \"/html/t/chart.tmpl\" dict \"Data\" .Agg.Count \"QueryString\" $query \"BaseUrl\" $base \"GlobalTotal\" $total \"Link\" false}}\n\n\n\n      <tr><td colspan=2><pre id=\"sample\" class=\"banner\">{{ banner \"sample\"}}</pre></td></tr>\n      {{ range .Sample }}\n      {{ template \"/html/t/hit.tmpl\" dict \"Data\" . \"QueryString\" $query \"BaseUrl\" $base}}\n      {{ end  }}\n    </table>\n  </body>\n</html>"

// Assets returns go-assets FileSystem
var Assets = assets.NewFileSystem(map[string][]string{"/": []string{"html"}, "/html": []string{}, "/html/t": []string{"hit.tmpl", "chart.tmpl", "index.tmpl"}}, map[string]*assets.File{
	"/html/t/chart.tmpl": &assets.File{
		Path:     "/html/t/chart.tmpl",
		FileMode: 0x1b4,
		Mtime:    time.Unix(1573646079, 1573646079252214920),
		Data:     []byte(_Assets6a465482d807e30a440fbe28a869d530372e9986),
	}, "/html/t/index.tmpl": &assets.File{
		Path:     "/html/t/index.tmpl",
		FileMode: 0x1b4,
		Mtime:    time.Unix(1573646261, 1573646261816824023),
		Data:     []byte(_Assets543aa4220323067c7b7aa789a3842050314b3ba3),
	}, "/": &assets.File{
		Path:     "/",
		FileMode: 0x800001fd,
		Mtime:    time.Unix(1573647077, 1573647077592376736),
		Data:     nil,
	}, "/html": &assets.File{
		Path:     "/html",
		FileMode: 0x800001fd,
		Mtime:    time.Unix(1573597030, 1573597030036383713),
		Data:     nil,
	}, "/html/t": &assets.File{
		Path:     "/html/t",
		FileMode: 0x800001fd,
		Mtime:    time.Unix(1573646261, 1573646261820824037),
		Data:     nil,
	}, "/html/t/hit.tmpl": &assets.File{
		Path:     "/html/t/hit.tmpl",
		FileMode: 0x1b4,
		Mtime:    time.Unix(1573641761, 1573641761010091377),
		Data:     []byte(_Assetsd675bab369b2bbedd927d7a44809bf92a4458c2f),
	}}, "")
