package io.georocket.output.xml

import io.georocket.storage.XMLChunkMeta
import rx.Completable
import rx.Single

/**
 * Merge chunks whose root XML elements are all equal
 * @author Michel Kraemer
 */
class AllSameStrategy : AbstractMergeStrategy() {
    override fun canMerge(meta: XMLChunkMeta): Single<Boolean> {
        return Single.defer {
            if (parents == null || parents == meta.parents) {
                return@Single.defer Single . just < Boolean >(java.lang.Boolean.TRUE)
            } else {
                return@Single.defer Single . just < Boolean >(java.lang.Boolean.FALSE)
            }
        }
    }

    override fun mergeParents(meta: XMLChunkMeta): Completable {
        if (parents == null) {
            parents = meta.parents
        }
        return Completable.complete()
    }
}
