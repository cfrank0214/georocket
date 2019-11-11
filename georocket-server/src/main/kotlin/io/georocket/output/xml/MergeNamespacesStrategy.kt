package io.georocket.output.xml

import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedHashMap
import java.util.regex.Pattern

import org.apache.commons.lang3.tuple.Pair

import com.google.common.base.Splitter

import io.georocket.storage.XMLChunkMeta
import io.georocket.util.XMLStartElement
import rx.Completable
import rx.Single

/**
 * Merge namespaces of XML root elements
 * @author Michel Kraemer
 */
class MergeNamespacesStrategy : AbstractMergeStrategy() {
    /**
     * The namespaces of the current XML root elements
     */
    private var currentNamespaces: MutableList<Map<String, String>>? = null

    /**
     * The attributes of the current XML root elements
     */
    private var currentAttributes: MutableList<Map<Pair<String, String>, String>>? = null

    override// collect namespaces from parents
    // collect attributes from parents
    var parents: List<XMLStartElement>
        get
        set(parents) {
            currentNamespaces = ArrayList()
            currentAttributes = ArrayList()
            for (e in parents) {
                val nss = LinkedHashMap<String, String>()
                currentNamespaces!!.add(nss)
                for (i in 0 until e.namespaceCount) {
                    var nsp1: String? = e.getNamespacePrefix(i)
                    val nsu1 = e.getNamespaceUri(i)
                    if (nsp1 == null) {
                        nsp1 = ""
                    }
                    nss[nsp1] = nsu1
                }
                val `as` = LinkedHashMap<Pair<String, String>, String>()
                currentAttributes!!.add(`as`)
                for (i in 0 until e.attributeCount) {
                    var ap: String? = e.getAttributePrefix(i)
                    val aln = e.getAttributeLocalName(i)
                    val av = e.getAttributeValue(i)
                    if (ap == null) {
                        ap = ""
                    }
                    `as`[Pair.of(ap, aln)] = av
                }
            }

            super.parents = parents
        }

    /**
     * Check if the namespaces of an XML start element can be merged in a map
     * of namespaces and if so update the map
     * @param namespaces the map of namespaces to update
     * @param e the element to merge
     * @param allowNew if the element may have namespaces that don't appear in
     * the given map
     * @return true if the element can be merged successfully
     */
    private fun canMergeNamespaces(namespaces: MutableMap<String, String>,
                                   e: XMLStartElement, allowNew: Boolean): Boolean {
        for (i in 0 until e.namespaceCount) {
            var nsp1: String? = e.getNamespacePrefix(i)
            val nsu1 = e.getNamespaceUri(i)
            if (nsp1 == null) {
                nsp1 = ""
            }
            val nsu2 = namespaces[nsp1]
            if (nsu2 == null) {
                if (allowNew) {
                    namespaces[nsp1] = nsu1
                } else {
                    return false
                }
            } else if (nsu1 != nsu2) {
                // found same prefix, but different URI
                return false
            }
        }
        return true
    }

    /**
     * Check if the attributes of an XML start element can be merge in a map
     * of attributes and if so update the map
     * @param attributes the map of attributes to update
     * @param e the element to merge
     * @param allowNew if the element may have attributes that don't appear in
     * the given map
     * @return true if the element can be merged successfully
     */
    private fun canMergeAttributes(attributes: MutableMap<String, String>,
                                   e: XMLStartElement, allowNew: Boolean): Boolean {
        for (i in 0 until e.attributeCount) {
            var ap1: String? = e.getAttributePrefix(i)
            val aln1 = e.getAttributeLocalName(i)
            val av1 = e.getAttributeValue(i)
            if (ap1 == null) {
                ap1 = ""
            }
            val name1 = "$ap1:$aln1"

            val av2 = attributes[name1]
            if (av2 == null) {
                if (allowNew) {
                    attributes[name1] = av1
                } else {
                    return false
                }
            } else {
                // ignore xsi:schemaLocation - we are able to merge this attribute
                if (name1 == "xsi:schemaLocation") {
                    continue
                }
                if (av1 != av2) {
                    // found duplicate attribute, but different value
                    return false
                }
            }
        }
        return true
    }

    /**
     * Check if two XML elements can be merged
     * @param e1 the first element
     * @param e2 the second element
     * @param allowNew true if e2 is allowed to have additional namespaces and
     * attributes that don't appear in e1
     * @return true if the elements can be merged
     */
    private fun canMerge(e1: XMLStartElement, e2: XMLStartElement,
                         allowNew: Boolean): Boolean {
        // check name
        if (e1.name != e2.name) {
            return false
        }

        // check namespaces
        val namespaces = HashMap<String, String>()
        if (!canMergeNamespaces(namespaces, e1, true)) {
            return false
        }
        if (!canMergeNamespaces(namespaces, e2, allowNew)) {
            return false
        }

        // check attributes
        val attributes = HashMap<String, String>()
        if (!canMergeAttributes(attributes, e1, true)) {
            return false
        }
        return if (!canMergeAttributes(attributes, e2, allowNew)) {
            false
        } else true

    }

    /**
     * Check if the given lists have the same size and all elements can be merged.
     * @param p1 the first list
     * @param p2 the second list
     * @param allowNew true if elements in p2 are allowed to have additional
     * namespaces and attributes that do not appear in the respective elements in p1
     * @return true if the two lists can be merged
     */
    private fun canMerge(p1: List<XMLStartElement>?, p2: List<XMLStartElement>?,
                         allowNew: Boolean): Boolean {
        if (p1 === p2) {
            return true
        }
        if (p1 == null || p2 == null) {
            return false
        }
        if (p1.size != p2.size) {
            return false
        }
        for (i in p1.indices) {
            if (!canMerge(p1[i], p2[i], allowNew)) {
                return false
            }
        }
        return true
    }

    override fun canMerge(meta: XMLChunkMeta): Single<Boolean> {
        return Single.defer {
            if (parents == null || canMerge(parents, meta.parents,
                            !isHeaderWritten)) {
                return@Single.defer Single . just < Boolean >(true)
            } else {
                return@Single.defer Single . just < Boolean >(false)
            }
        }
    }

    override fun mergeParents(meta: XMLChunkMeta): Completable {
        if (parents == null) {
            // no merge necessary yet, just save the chunk's parents
            parents = meta.parents
            return Completable.complete()
        }

        // merge current parents and chunk parents
        val newParents = ArrayList<XMLStartElement>()
        var changed = false
        for (i in 0 until meta.parents.size) {
            val p = meta.parents[i]
            val currentNamespaces = this.currentNamespaces!![i]
            val currentAttributes = this.currentAttributes!![i]
            var newParent = mergeParent(p, currentNamespaces, currentAttributes)
            if (newParent == null) {
                newParent = this.parents[i]
            } else {
                changed = true
            }
            newParents.add(newParent)
        }
        if (changed) {
            super.parents = newParents
        }

        return Completable.complete()
    }

    /**
     * Merge an XML start element into a map of namespaces and a map of attributes
     * @param e the XML start element to merge
     * @param namespaces the current namespaces to merge into
     * @param attributes the current attributes to merge into
     * @return the merged element or `null` if no merge was necessary
     */
    private fun mergeParent(e: XMLStartElement,
                            namespaces: MutableMap<String, String>, attributes: MutableMap<Pair<String, String>, String>): XMLStartElement? {
        var changed = false

        // merge namespaces
        for (i in 0 until e.namespaceCount) {
            var nsp: String? = e.getNamespacePrefix(i)
            if (nsp == null) {
                nsp = ""
            }
            if (!namespaces.containsKey(nsp)) {
                val nsu = e.getNamespaceUri(i)
                namespaces[nsp] = nsu
                changed = true
            }
        }

        // merge attributes
        for (i in 0 until e.attributeCount) {
            var ap: String? = e.getAttributePrefix(i)
            val aln = e.getAttributeLocalName(i)
            if (ap == null) {
                ap = ""
            }
            val name = Pair.of(ap, aln)
            if (!attributes.containsKey(name)) {
                // add new attribute
                val av = e.getAttributeValue(i)
                attributes[name] = av
                changed = true
            } else if (ap == "xsi" && aln == "schemaLocation") {
                // merge schema location
                val av = e.getAttributeValue(i)

                // find new schema locations and convert them to regular expressions
                val avList = Splitter.on(' ')
                        .trimResults()
                        .omitEmptyStrings()
                        .splitToList(av)
                val avRegExs = ArrayList<Pair<String, String>>()
                var j = 0
                while (j < avList.size) {
                    var v = avList[j]
                    var r = Pattern.quote(v)
                    if (j + 1 < avList.size) {
                        val v2 = avList[j + 1]
                        v += " $v2"
                        r += "\\s+" + Pattern.quote(v2)
                    }
                    avRegExs.add(Pair.of(r, v))
                    j += 2
                }

                // test which new schema locations already exist in the
                // previous attribute value
                val existingAv = attributes[name]
                var newAv = ""
                for ((key, value) in avRegExs) {
                    val pattern = Pattern.compile(key)
                    if (!pattern.matcher(existingAv).find()) {
                        newAv += " $value"
                    }
                }

                // merge attribute values
                if (!newAv.isEmpty()) {
                    attributes[name] = existingAv + newAv
                    changed = true
                }
            }
        }

        return if (!changed) {
            // no need to create a new parent
            null
        } else XMLStartElement(e.prefix, e.localName,
                namespaces.keys.toTypedArray(),
                namespaces.values.toTypedArray(),
                attributes.keys.stream().map { p -> p.key }.toArray(String[]::new  /* Currently unsupported in Kotlin */),
                attributes.keys.stream().map { p -> p.value }.toArray(String[]::new  /* Currently unsupported in Kotlin */),
                attributes.values.toTypedArray())

        // create new merged parent
    }
}
