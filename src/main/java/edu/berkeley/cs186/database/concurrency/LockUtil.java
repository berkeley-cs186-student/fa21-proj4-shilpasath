package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import java.util.ArrayList;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);
        // TODO(proj4_part2): implement
        LockType wantedParentType = LockType.parentLock(requestType);
        if (requestType == LockType.NL || requestType.equals(explicitLockType)) {
            return;
        }
//        else if (lockContext.parent != null) {
//            promoteParent(lockContext, transaction, wantedParentType);
//        }
        //     * - The current lock type can effectively substitute the requested type
        if (explicitLockType == LockType.NL) {
            promoteParent(lockContext, transaction, wantedParentType);
            lockContext.acquire(transaction, requestType);
        } else if (LockType.substitutable(requestType, explicitLockType)) {
            promoteParent(lockContext, transaction, wantedParentType);
            lockContext.promote(transaction, requestType);
        } else if ((explicitLockType == LockType.IS && requestType == LockType.S)
                || (explicitLockType == LockType.IX && requestType == LockType.X)
                || (explicitLockType == LockType.SIX && requestType == LockType.X)) {
            promoteParent(lockContext, transaction, wantedParentType);
            lockContext.escalate(transaction);
        } else if (explicitLockType == LockType.SIX && requestType == LockType.S) {
            return;
        } else if (explicitLockType == LockType.IX && requestType == LockType.S){
                lockContext.release(transaction);
                lockContext.acquire(transaction, LockType.SIX);
        } else {
            promoteParent(lockContext, transaction, wantedParentType);
            lockContext.release(transaction);
            lockContext.acquire(transaction, requestType);
        }

        for (Lock l : lockContext.lockman.getLocks(transaction)) {
            if (l.name.isDescendantOf(lockContext.name)) {
                lockContext.fromResourceName(lockContext.lockman, l.name).release(transaction);
            }
        }

            //     * - The current lock type is IX and the requested lock is S
            //     * - The current lock type is an intent lock








        //TASK1: ensure that we have the appropriate locks on ancestors
        ArrayList<LockContext> ancestors = findAncestors(lockContext);

        //im thinking we do all these checks to compare the wanted parent type to each ancestor,
        //then do the same for current type and requestType



        for (LockContext ancestor : ancestors) {
            //promote when:
            //parent needs to be SIX, and ancestor is IS/S/ IX -- unsure ab IX
            //

            //escalate parent when ancestor is IS or IX

            //if they fail, acquire

        }

        //escalate when we have child locks that would become redundant
        // if we were to acquire a lock of requestType (S or X) without releasing the children





        // acquiring the lock on the resource
        return;
    }

    // TODO(proj4_part2) add any helper methods you want
    public static ArrayList<LockContext> findAncestors(LockContext lockContext) {
        ArrayList<LockContext> ancestors = new ArrayList<>();
        LockContext parent = lockContext.parentContext();

        while (parent != null) {
            //add at index 0 because you want the highest ancestor first
            ancestors.add(0 ,parent);
            parent = parent.parentContext();
        }
        return ancestors;
    }


    public static void promoteParent(LockContext lockContext, TransactionContext transaction, LockType wantedParentType) {
        if (lockContext.parent == null) {
            return;
        }
        ArrayList<LockContext> ancestors = findAncestors(lockContext);
        for (LockContext a : ancestors) {
            if (a.getExplicitLockType(transaction) == wantedParentType) {
                continue;
            } else if (a.getExplicitLockType(transaction) != LockType.NL) {
                a.promote(transaction, wantedParentType);
            } else {
                a.acquire(transaction, wantedParentType);
            }
        }
    }
}
