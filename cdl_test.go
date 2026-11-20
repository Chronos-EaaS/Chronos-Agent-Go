package agent

import "testing"

func TestParseCDLPreservesExactParameterText(t *testing.T) {
	raw := "\uFEFF" + `<?xml version="1.0" encoding="utf-8"?>
<chronos>
  <setup><empty/></setup>
  <evaluation system="1">
    <payload>  A&amp;B &lt; &amp;amp; Zürich &#13; <![CDATA[  <raw>]]></payload>
    <größe>0</größe>
  </evaluation>
</chronos>`
	cdl, err := ParseCDL(raw)
	if err != nil || cdl.System != "1" || cdl.Evaluation["payload"] != "  A&B < &amp; Zürich \r   <raw>" ||
		cdl.Evaluation["größe"] != "0" || cdl.Setup["empty"] != "" || len(cdl.Setup) != 1 {
		t.Fatalf("CDL = %+v, err = %v", cdl, err)
	}
}

func TestParseCDLRejectsMalformedAndAmbiguousDocuments(t *testing.T) {
	for _, raw := range []string{
		"",
		"<chronos><setup>",
		"<chronos><setup/><evaluation/></chronos>",
		"<chronos><evaluation system='1'/></chronos>",
		"<chronos><setup/><setup/><evaluation system='1'/></chronos>",
		"<chronos><setup/><evaluation system='1'><x/><x/></evaluation></chronos>",
		"<chronos><setup/><evaluation system='1'><x><y/></x></evaluation></chronos>",
		"<chronos><setup/><evaluation system='1'><x k='v'/></evaluation></chronos>",
		"<chronos><setup/><evaluation system='1'><x>&unresolved;</x></evaluation></chronos>",
		"<chronos><setup/>text<evaluation system='1'/></chronos>",
		"<chronos><setup/><evaluation system='1'/></chronos><other/>",
		"<chronos><setup/><evaluation system='1'/></chronos>trailing",
		"<!DOCTYPE chronos [<!ENTITY value SYSTEM 'file:///tmp/sample'>]><chronos><setup/><evaluation system='1'/></chronos>",
		"<chronos xmlns='urn:test'><setup/><evaluation system='1'/></chronos>",
		"<chronos><prefix:setup/><evaluation system='1'/></chronos>",
	} {
		if _, err := ParseCDL(raw); err == nil {
			t.Errorf("accepted %q", raw)
		}
	}
}
