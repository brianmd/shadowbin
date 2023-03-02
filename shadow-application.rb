#!/usr/bin/env ruby

require_relative 'shadow-bin'
include TtdShadowBin



def application_code(user_doc_definition)
  warn "\n---------- read from db"
  doc = user_doc_definition.read(1)
  show_doc(doc)

  warn "\n\n---------- change fmap"
  doc['fmap'] = 'new-fmap-bin'
  show_doc(doc)

  doc.save
end



def build_user_document_template
  host = '127.0.0.1:3000'
  cluster = TtdClusterAerospike.new(host)
  user_ns = TtdCorpus.new(cluster, 'ttd-user')
  user_set = TtdSet.new(user_ns, 'User')
  doc_def = TtdDocumentDefinition.new

  xdevice_users2 = TtdAttr.new(user_set, 'XDeviceUsers2')
  g = TtdAttr.new(user_set, 'G')
  user_metadata = TtdAttr.new(user_set, 'UserMetadata')
  user_target2 = TtdAttr.new(user_set, 'UserTarget2')
  ad_fmap2 = TtdAttr.new(user_set, 'AdFMap2')       # same as below

  # This code moves the fmap bin in namespace ttd-fcap.
  if false
    user_fmap_ns = TtdCorpus.new(cluster, 'ttd-fcap')
    user_fmap_set = TtdSet.new(user_fmap_ns, 'f')
    orig_ad_fmap2 = TtdAttr.new(user_set, 'AdFMap2')  # same as above
    ad_fmap2 = TtdAttr.new(user_fmap_set, 'f')
    ad_fmap2.shadow_bin = orig_ad_fmap2
  end

  doc_def['xdevice_users'] = xdevice_users2
  doc_def['g'] = g
  doc_def['metadata'] = user_metadata
  doc_def['targets'] = user_target2
  doc_def['fmap'] = ad_fmap2
  doc_def
end

def show_doc(doc)
  puts "to_h:#{doc.to_h}"
end


user_doc_template = build_user_document_template
application_code(user_doc_template)
