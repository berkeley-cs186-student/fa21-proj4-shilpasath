package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import java.util.ArrayList;
import java.util.Arrays;
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

        // parents
        if (parentContext != null && !(explicitLockType == LockType.IX
                && requestType == LockType.S) && !LockType.canBeParentLock(parentContext.getExplicitLockType(transaction), requestType)) {
            promoteParents(lockContext, transaction, wantedParentType);
        }
        // kids
        boolean releaseChildren = false;

        if (explicitLockType == LockType.NL) {
            lockContext.acquire(transaction, requestType);
            releaseChildren = true;
        } else if (LockType.substitutable(requestType, explicitLockType)) {
            lockContext.promote(transaction, requestType);
            releaseChildren = true;
        } else if ((explicitLockType == LockType.IS && requestType == LockType.S)
                || (explicitLockType == LockType.IX && requestType == LockType.X)
                || (explicitLockType == LockType.SIX && requestType == LockType.X)) {
            lockContext.escalate(transaction);
        } else if (explicitLockType == LockType.SIX && requestType == LockType.S) {
            return;
        } else if (explicitLockType == LockType.IX && requestType == LockType.S){ // this is wrong
//            lockContext.release(transaction);
//            lockContext.acquire(transaction, LockType.SIX);
            lockContext.promote(transaction, LockType.SIX);
        } else {
            lockContext.release(transaction);
            lockContext.acquire(transaction, requestType);
//            lockContext.lockman.acquireAndRelease(transaction, lockContext.name, requestType, new ArrayList<>(Arrays.asList(lockContext.name)));
            releaseChildren = true;
        }

        if (releaseChildren) {
            for (Lock l : lockContext.lockman.getLocks(transaction)) {
                if (l.name.isDescendantOf(lockContext.name)) {
                    lockContext.fromResourceName(lockContext.lockman, l.name).release(transaction);
                }
            }
        }
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

    public static void promoteParents(LockContext lockContext, TransactionContext transaction, LockType wantedParentType) {
        if (lockContext == null) {
            return;
        }

        LockContext parent = lockContext.parentContext();
        if (parent == null || parent.getExplicitLockType(transaction) == wantedParentType) {
            return;
        }
        promoteParents(lockContext.parentContext(), transaction, LockType.parentLock(wantedParentType));

        if (parent.getExplicitLockType(transaction) == LockType.NL) {
            parent.acquire(transaction, wantedParentType);
        } else {
            try {
                parent.promote(transaction, wantedParentType);
            } catch (DuplicateLockRequestException | NoLockHeldException e) {
                System.out.println(e);
                parent.release(transaction);
                parent.acquire(transaction, wantedParentType);
            }
        }
    }
}
